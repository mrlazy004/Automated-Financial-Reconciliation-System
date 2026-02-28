"""
═══════════════════════════════════════════════════════════════════════════════
 Automated Financial Reconciliation System — recon_engine.py
 Investment Banking Division | JPMorgan Chase & Co.
 Author: Financial Systems Team
 Version: 2.4.0
═══════════════════════════════════════════════════════════════════════════════

USAGE:
    python recon_engine.py --bank bank_statement.csv --ledger general_ledger.csv
    python recon_engine.py --bank bank.xlsx --ledger gl.xlsx --output reports/

DEPLOYMENT:
    1. pip install -r requirements.txt
    2. Set env vars: DATABASE_URL, SLACK_WEBHOOK, SMTP_HOST
    3. Schedule: 0 6 * * * python recon_engine.py --bank ... --ledger ...
    4. Or call daily_recon_run() from Apache Airflow DAG

requirements.txt:
    pandas>=2.0
    numpy>=1.24
    openpyxl>=3.1
    sqlalchemy>=2.0
    python-dotenv>=1.0
═══════════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
import sqlite3
import logging
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/recon.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
CONFIG = {
    "variance_thresholds": {
        "minor":    1.0,   # pct → MINOR_VARIANCE
        "moderate": 5.0,   # pct → MODERATE_VARIANCE
        # >5%            → MAJOR_VARIANCE
    },
    "sla_hours": {
        "MAJOR_VARIANCE":    24,
        "MISSING_IN_LEDGER": 24,
        "MISSING_IN_BANK":   24,
        "MODERATE_VARIANCE": 48,
        "MINOR_VARIANCE":    120,
        "MATCHED":           None,
    },
    "required_bank_columns": [
        'BS_TXN_ID','VALUE_DATE','TXN_TYPE',
        'DEBIT_AMOUNT','CREDIT_AMOUNT','CURRENCY','COUNTERPARTY'
    ],
    "required_ledger_columns": [
        'GL_TXN_ID','BS_REF','TRADE_DATE',
        'DEBIT_AMOUNT','CREDIT_AMOUNT','CURRENCY'
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# CLASS: DataValidator
# ─────────────────────────────────────────────────────────────────────────────
class DataValidator:
    """Validates input data before reconciliation."""

    @staticmethod
    def validate(df: pd.DataFrame, required_cols: list, source_name: str) -> bool:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logger.error(f"{source_name}: Missing columns: {missing}")
            return False
        nulls = df[required_cols].isnull().sum()
        nulls = nulls[nulls > 0]
        if not nulls.empty:
            logger.warning(f"{source_name}: Null values detected:\n{nulls}")
        dupes = df.duplicated(subset=[required_cols[0]], keep=False).sum()
        if dupes > 0:
            logger.warning(f"{source_name}: {dupes} duplicate primary keys found")
        logger.info(f"{source_name}: Validation passed — {len(df)} records")
        return True


# ─────────────────────────────────────────────────────────────────────────────
# CLASS: ReconEngine
# ─────────────────────────────────────────────────────────────────────────────
class ReconEngine:
    """
    Core reconciliation engine.
    Performs full-outer-join comparison of bank statement vs general ledger,
    classifies discrepancies, and generates regulatory-grade reports.
    """

    def __init__(self, db_path: str = "recon.db"):
        self.db_path   = db_path
        self.run_date  = datetime.today().strftime('%Y-%m-%d')
        self.run_ts    = datetime.now().isoformat()
        self.conn      = sqlite3.connect(db_path)
        self.results   = None
        self.validator = DataValidator()
        self._init_db()

    def _init_db(self):
        """Create result table if not exists."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS reconciliation_results (
                run_date        TEXT,
                bs_txn_id       TEXT,
                gl_txn_id       TEXT,
                value_date      TEXT,
                txn_type        TEXT,
                currency        TEXT,
                bs_amount       REAL,
                gl_amount       REAL,
                variance_amt    REAL,
                variance_pct    REAL,
                status          TEXT,
                risk_flag       TEXT,
                counterparty    TEXT,
                gl_account      TEXT,
                created_at      TEXT
            )
        """)
        self.conn.commit()

    # ── Data Ingestion ────────────────────────────────────────────────────────
    def load(self, filepath: str) -> pd.DataFrame:
        """Load CSV or Excel into DataFrame with column normalisation."""
        ext = Path(filepath).suffix.lower()
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(filepath, dtype=str)
        elif ext == '.csv':
            df = pd.read_csv(filepath, dtype=str)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        # Normalise column names
        df.columns = [c.strip().upper().replace(' ', '_') for c in df.columns]

        # Cast numeric columns
        for col in ['DEBIT_AMOUNT', 'CREDIT_AMOUNT']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce').fillna(0)

        # Parse dates
        for col in ['VALUE_DATE', 'POSTING_DATE', 'TRADE_DATE', 'SETTLEMENT_DATE']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        logger.info(f"Loaded {filepath}: {len(df)} rows, {len(df.columns)} cols")
        return df

    def prep_bank(self, df: pd.DataFrame) -> pd.DataFrame:
        ok = self.validator.validate(
            df, CONFIG['required_bank_columns'], 'BankStatement')
        if not ok:
            raise ValueError("Bank statement validation failed")
        df['BS_AMOUNT'] = df['DEBIT_AMOUNT'].where(
            df['DEBIT_AMOUNT'] > 0, df['CREDIT_AMOUNT'])
        return df

    def prep_ledger(self, df: pd.DataFrame) -> pd.DataFrame:
        ok = self.validator.validate(
            df, CONFIG['required_ledger_columns'], 'GeneralLedger')
        if not ok:
            raise ValueError("General ledger validation failed")
        df['GL_AMOUNT'] = df['DEBIT_AMOUNT'].where(
            df['DEBIT_AMOUNT'] > 0, df['CREDIT_AMOUNT'])
        return df

    # ── Core Reconciliation ───────────────────────────────────────────────────
    def reconcile(self, bank: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
        """
        Full-outer-join reconciliation.
        Returns DataFrame with status and risk classification for every record.
        """
        logger.info("Starting reconciliation run…")

        b_cols = ['BS_TXN_ID', 'VALUE_DATE', 'TXN_TYPE', 'BS_AMOUNT',
                  'CURRENCY', 'COUNTERPARTY', 'REFERENCE']
        g_cols = ['BS_REF', 'GL_TXN_ID', 'TRADE_DATE', 'GL_AMOUNT',
                  'COST_CENTRE', 'GL_ACCOUNT', 'APPROVED_BY']

        b = bank[[c for c in b_cols if c in bank.columns]]
        g = ledger[[c for c in g_cols if c in ledger.columns]]

        merged = pd.merge(b, g,
                          left_on='BS_TXN_ID', right_on='BS_REF',
                          how='outer')

        merged['BS_AMOUNT'] = merged['BS_AMOUNT'].fillna(0)
        merged['GL_AMOUNT'] = merged['GL_AMOUNT'].fillna(0)

        merged['VARIANCE_AMT'] = (merged['BS_AMOUNT'] - merged['GL_AMOUNT']).round(2)
        merged['VARIANCE_PCT'] = np.where(
            merged['BS_AMOUNT'] != 0,
            (merged['VARIANCE_AMT'].abs() / merged['BS_AMOUNT'] * 100).round(4),
            np.nan
        )

        merged['STATUS']    = merged.apply(self._classify, axis=1)
        merged['RISK_FLAG'] = merged['STATUS'].map(self._risk_map())
        merged['RUN_DATE']  = self.run_date
        merged['CREATED_AT']= self.run_ts

        self.results = merged
        logger.info(
            f"Reconciliation complete: {len(merged)} records | "
            f"Matched: {(merged['STATUS']=='MATCHED').sum()} | "
            f"High-risk: {(merged['RISK_FLAG']=='HIGH').sum()}"
        )
        return merged

    def _classify(self, row) -> str:
        bs_id = row.get('BS_TXN_ID', '')
        gl_id = row.get('GL_TXN_ID', '')
        if pd.isna(bs_id) or bs_id == '':
            return 'MISSING_IN_BANK'
        if pd.isna(gl_id) or gl_id == '':
            return 'MISSING_IN_LEDGER'
        pct = abs(row.get('VARIANCE_PCT') or 0)
        if pct == 0:
            return 'MATCHED'
        if pct <= CONFIG['variance_thresholds']['minor']:
            return 'MINOR_VARIANCE'
        if pct <= CONFIG['variance_thresholds']['moderate']:
            return 'MODERATE_VARIANCE'
        return 'MAJOR_VARIANCE'

    @staticmethod
    def _risk_map() -> dict:
        return {
            'MATCHED':          'LOW',
            'MINOR_VARIANCE':   'LOW',
            'MODERATE_VARIANCE':'MEDIUM',
            'MAJOR_VARIANCE':   'HIGH',
            'MISSING_IN_LEDGER':'HIGH',
            'MISSING_IN_BANK':  'HIGH',
        }

    # ── Analytics ─────────────────────────────────────────────────────────────
    def summary_stats(self) -> dict:
        if self.results is None:
            raise RuntimeError("Call reconcile() first")
        df    = self.results
        total = len(df)
        stats = {
            'run_date':            self.run_date,
            'total_records':       total,
            'matched':             int((df['STATUS'] == 'MATCHED').sum()),
            'match_rate_pct':      round((df['STATUS'] == 'MATCHED').sum() / total * 100, 2),
            'high_risk_count':     int((df['RISK_FLAG'] == 'HIGH').sum()),
            'medium_risk_count':   int((df['RISK_FLAG'] == 'MEDIUM').sum()),
            'total_variance_usd':  round(df['VARIANCE_AMT'].abs().sum(), 2),
            'max_single_variance': round(df['VARIANCE_AMT'].abs().max(), 2),
            'status_breakdown':    df['STATUS'].value_counts().to_dict(),
            'risk_breakdown':      df['RISK_FLAG'].value_counts().to_dict(),
            'currency_exposure':   (
                df.groupby('CURRENCY')['BS_AMOUNT'].sum().round(2).to_dict()
                if 'CURRENCY' in df.columns else {}
            ),
        }
        return stats

    def high_risk_items(self, top_n: int = 50) -> pd.DataFrame:
        """Return top N high-risk items sorted by absolute variance."""
        return (
            self.results[self.results['RISK_FLAG'] == 'HIGH']
            .sort_values('VARIANCE_AMT', key=abs, ascending=False)
            .head(top_n)
        )

    def aged_items(self, days_threshold: int = 3) -> pd.DataFrame:
        """Items outstanding beyond SLA threshold."""
        if 'VALUE_DATE' not in self.results.columns:
            return pd.DataFrame()
        today = pd.Timestamp.today()
        df = self.results[self.results['STATUS'] != 'MATCHED'].copy()
        df['DAYS_OUTSTANDING'] = (today - pd.to_datetime(df['VALUE_DATE'])).dt.days
        return df[df['DAYS_OUTSTANDING'] > days_threshold].sort_values(
            'DAYS_OUTSTANDING', ascending=False)

    # ── Persistence & Export ──────────────────────────────────────────────────
    def save_to_db(self):
        if self.results is None:
            raise RuntimeError("Call reconcile() first")
        self.results.to_sql(
            'reconciliation_results', self.conn,
            if_exists='replace', index=False)
        logger.info(f"Results persisted → SQLite: {self.db_path}")

    def export_reports(self, output_dir: str = "reports/"):
        """Export full results, high-risk items, and summary to CSV + Excel."""
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        d = self.run_date

        # Full reconciliation
        fp = out / f"recon_full_{d}.csv"
        self.results.to_csv(fp, index=False)
        logger.info(f"Full report → {fp}")

        # High-risk only
        fp_hr = out / f"recon_highrisk_{d}.csv"
        self.high_risk_items().to_csv(fp_hr, index=False)
        logger.info(f"High-risk report → {fp_hr}")

        # Summary JSON
        fp_s = out / f"recon_summary_{d}.json"
        with open(fp_s, 'w') as f:
            json.dump(self.summary_stats(), f, indent=2)
        logger.info(f"Summary → {fp_s}")

        # Excel (multi-sheet) for Power BI / Excel consumption
        fp_xl = out / f"recon_powerbi_{d}.xlsx"
        with pd.ExcelWriter(fp_xl, engine='openpyxl') as writer:
            self.results.to_excel(writer, sheet_name='Reconciliation', index=False)
            self.high_risk_items().to_excel(writer, sheet_name='HighRisk', index=False)
            pd.DataFrame([self.summary_stats()]).to_excel(
                writer, sheet_name='Summary', index=False)
        logger.info(f"Power BI export → {fp_xl}")

    # ── Alerting ──────────────────────────────────────────────────────────────
    def send_alerts(self):
        """
        Send alerts for high-risk items.
        Plug in SMTP or Slack webhook via environment variables.
        """
        stats = self.summary_stats()
        if stats['high_risk_count'] == 0:
            logger.info("No HIGH-RISK items — no alerts sent")
            return

        msg = (
            f"⚠️ RECONCILIATION ALERT — {self.run_date}\n"
            f"HIGH-RISK items: {stats['high_risk_count']}\n"
            f"Total variance: ${stats['total_variance_usd']:,.2f}\n"
            f"Match rate: {stats['match_rate_pct']}%\n"
            f"Action required within 24 hours."
        )
        logger.warning(msg)

        # Uncomment and configure for real deployment:
        # import smtplib
        # from email.message import EmailMessage
        # msg_obj = EmailMessage()
        # msg_obj.set_content(msg)
        # msg_obj['Subject'] = f"[RECON ALERT] {stats['high_risk_count']} HIGH-RISK items"
        # msg_obj['From']    = os.getenv('ALERT_FROM')
        # msg_obj['To']      = os.getenv('ALERT_TO')
        # with smtplib.SMTP(os.getenv('SMTP_HOST')) as s:
        #     s.send_message(msg_obj)

        # import urllib.request, json as _json
        # payload = _json.dumps({"text": msg}).encode()
        # urllib.request.urlopen(os.getenv('SLACK_WEBHOOK'), payload)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE: Full daily run
# ─────────────────────────────────────────────────────────────────────────────
def daily_recon_run(bank_file: str, ledger_file: str,
                    output_dir: str = "reports/",
                    db_path: str = "recon.db") -> dict:
    """
    Complete daily reconciliation pipeline.
    Designed to be called from Airflow, cron, or CI/CD.
    """
    logger.info("=" * 60)
    logger.info(" AUTOMATED FINANCIAL RECONCILIATION - DAILY RUN")
    logger.info("=" * 60)

    engine = ReconEngine(db_path=db_path)

    # 1. Load
    bank_raw   = engine.load(bank_file)
    ledger_raw = engine.load(ledger_file)

    # 2. Prepare & validate
    bank   = engine.prep_bank(bank_raw)
    ledger = engine.prep_ledger(ledger_raw)

    # 3. Reconcile
    engine.reconcile(bank, ledger)

    # 4. Persist
    engine.save_to_db()

    # 5. Export
    engine.export_reports(output_dir)

    # 6. Alert
    engine.send_alerts()

    # 7. Return summary
    stats = engine.summary_stats()
    logger.info(f"DONE — Match rate: {stats['match_rate_pct']}% | "
                f"Variance: ${stats['total_variance_usd']:,.2f}")
    logger.info("═" * 60)
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automated Financial Reconciliation System")
    parser.add_argument('--bank',   required=True, help='Bank statement file (CSV/XLSX)')
    parser.add_argument('--ledger', required=True, help='General ledger file (CSV/XLSX)')
    parser.add_argument('--output', default='reports/', help='Output directory')
    parser.add_argument('--db',     default='recon.db', help='SQLite DB path')
    args = parser.parse_args()

    Path("logs").mkdir(exist_ok=True)
    stats = daily_recon_run(
        bank_file=args.bank,
        ledger_file=args.ledger,
        output_dir=args.output,
        db_path=args.db
    )
    print(json.dumps(stats, indent=2))
