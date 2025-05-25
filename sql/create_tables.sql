DROP TABLE IF EXISTS daily_bonus_payouts CASCADE;
DROP TABLE IF EXISTS daily_interest_rates CASCADE;
DROP TABLE IF EXISTS wallet_history CASCADE;
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY 
);

CREATE TABLE wallet_history (
    history_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    balance NUMERIC(18, 2) NOT NULL, 

    UNIQUE (user_id, timestamp)
);

CREATE INDEX idx_wallet_history_user_timestamp ON wallet_history (user_id, timestamp);

CREATE TABLE daily_interest_rates (
    rate_date DATE PRIMARY KEY,
    daily_rate NUMERIC(10, 8) NOT NULL
);

CREATE TABLE daily_bonus_payouts (
    payout_id SERIAL PRIMARY KEY,
    payout_date DATE NOT NULL,
    user_id INTEGER NOT NULL REFERENCES users(user_id), 
    calculated_amount NUMERIC(18, 2) NOT NULL, 

    UNIQUE (payout_date, user_id)
);

CREATE INDEX idx_daily_bonus_payouts_date_user ON daily_bonus_payouts (payout_date, user_id);

COMMENT ON TABLE users IS 'Stores basic user information.';
COMMENT ON TABLE wallet_history IS 'Stores the historical balance of user wallets based on CDC data.';
COMMENT ON TABLE daily_interest_rates IS 'Stores the daily CDI interest rates.';
COMMENT ON TABLE daily_bonus_payouts IS 'Records the daily calculated CDI bonus amount for each user.';
