CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name TEXT,
    region TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT,
    amount NUMERIC,
    order_date DATE
);

-- Seed data (safe to rerun)
INSERT INTO customers (name, region) VALUES
('Ava Johnson', 'West'),
('Rahul Sharma', 'East'),
('Maria Garcia', 'South')
ON CONFLICT DO NOTHING;

INSERT INTO orders (customer_id, amount, order_date) VALUES
(1, 120.50, '2025-01-10'),
(1, 80.00,  '2025-01-14'),
(2, 250.00, '2025-01-16'),
(3, 60.75,  '2025-01-20')
ON CONFLICT DO NOTHING;
