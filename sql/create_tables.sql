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
    daily_rate NUMERIC(18, 8) NOT NULL
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

-- Comments for users table columns
COMMENT ON COLUMN users.user_id IS 'Unique identifier for each user in the system';

-- Comments for wallet_history table columns
COMMENT ON COLUMN wallet_history.history_id IS 'Unique identifier for each wallet history record';
COMMENT ON COLUMN wallet_history.user_id IS 'Reference to the user who owns this wallet record';
COMMENT ON COLUMN wallet_history.timestamp IS 'Exact date and time of the balance change, used for tracking 24-hour holding periods';
COMMENT ON COLUMN wallet_history.balance IS 'Point-in-time wallet balance after transaction, with 2 decimal places. Used to determine bonus eligibility (>100) and calculate CDI amounts';

-- Comments for daily_interest_rates table columns
COMMENT ON COLUMN daily_interest_rates.rate_date IS 'Reference date for the CDI rate. If no rate exists, system looks back up to 90 days for the most recent rate';
COMMENT ON COLUMN daily_interest_rates.daily_rate IS 'Daily CDI interest rate (converted from annual rate using power of 1/365) with 8 decimal precision';

-- Comments for daily_bonus_payouts table columns
COMMENT ON COLUMN daily_bonus_payouts.payout_id IS 'Unique identifier for each bonus payout calculation';
COMMENT ON COLUMN daily_bonus_payouts.payout_date IS 'Date for which the bonus was calculated. Only balances held for over 24 hours before this date are eligible';
COMMENT ON COLUMN daily_bonus_payouts.user_id IS 'Reference to the user receiving the bonus. User must maintain balance >100 and no movements in last 24h to be eligible';
COMMENT ON COLUMN daily_bonus_payouts.calculated_amount IS 'Daily CDI bonus amount (balance * daily_rate) with 2 decimal places, calculated only for eligible users meeting all criteria';