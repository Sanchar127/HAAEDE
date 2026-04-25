import asyncpg
import os

DB_URL = os.getenv("POSTGRES_DSN")

async def save_features(features: dict):
    conn = await asyncpg.connect(DB_URL)

    await conn.execute(
        """
        INSERT INTO features (
            customer_id,
            payment_frequency_30d,
            avg_payment_amount,
            days_since_last_payment,
            delinquency_ratio
        )
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (customer_id)
        DO UPDATE SET
            payment_frequency_30d = EXCLUDED.payment_frequency_30d,
            avg_payment_amount = EXCLUDED.avg_payment_amount,
            days_since_last_payment = EXCLUDED.days_since_last_payment,
            delinquency_ratio = EXCLUDED.delinquency_ratio
        """,
        features["customer_id"],
        features["payment_frequency_30d"],
        features["avg_payment_amount"],
        features["days_since_last_payment"],
        features["delinquency_ratio"]
    )

    await conn.close()