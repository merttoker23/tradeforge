-- Frozen schema v1. All monetary values are exact, unbounded NUMERIC (no silent rounding).
CREATE FUNCTION finite_numeric(numeric) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT $1 NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric) $$;

CREATE FUNCTION append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'APPEND_ONLY' USING ERRCODE = '23514'; END $$;

CREATE FUNCTION version_successor() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'INVALID_LIFECYCLE_VERSION' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TABLE instruments (
    instrument_id uuid PRIMARY KEY,
    exchange text NOT NULL CHECK (exchange = 'BINANCE'),
    market_type text NOT NULL CHECK (market_type = 'USD_M'),
    symbol text NOT NULL CHECK (symbol ~ '^[A-Z0-9]+USDT$'),
    contract_type text NOT NULL CHECK (contract_type = 'PERPETUAL'),
    quote text NOT NULL DEFAULT 'USDT' CHECK (quote = 'USDT'),
    margin_asset text NOT NULL DEFAULT 'USDT' CHECK (margin_asset = 'USDT'),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    observed_at timestamptz NOT NULL,
    UNIQUE (exchange, market_type, symbol, contract_type)
);
CREATE TABLE universe_snapshots (
    snapshot_id uuid PRIMARY KEY,
    version_hash text NOT NULL UNIQUE CHECK (version_hash ~ '^[a-f0-9]{64}$'),
    as_of timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    payload jsonb NOT NULL
);
CREATE INDEX universe_as_of ON universe_snapshots (as_of);
CREATE TABLE closed_bars (
    instrument_id uuid NOT NULL REFERENCES instruments,
    timeframe text NOT NULL CHECK (timeframe IN ('1m','5m','15m','1h','4h','1d')),
    open_at timestamptz NOT NULL,
    close_at timestamptz NOT NULL CHECK (close_at >= open_at),
    observed_at timestamptz NOT NULL CHECK (observed_at >= close_at),
    open numeric NOT NULL CHECK (finite_numeric(open) AND open > 0),
    high numeric NOT NULL CHECK (finite_numeric(high) AND high >= open),
    low numeric NOT NULL CHECK (finite_numeric(low) AND low > 0 AND low <= open),
    close numeric NOT NULL CHECK (finite_numeric(close) AND close BETWEEN low AND high),
    volume numeric NOT NULL CHECK (finite_numeric(volume) AND volume >= 0),
    complete boolean NOT NULL CHECK (complete),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    PRIMARY KEY (instrument_id, timeframe, open_at)
);
CREATE INDEX bars_observed ON closed_bars (observed_at);
CREATE TABLE feature_snapshots (
    snapshot_id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES instruments,
    feature_hash text NOT NULL CHECK (feature_hash ~ '^[a-f0-9]{64}$'),
    as_of timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    payload jsonb NOT NULL,
    UNIQUE (instrument_id, feature_hash, as_of)
);
CREATE TABLE regime_snapshots (
    snapshot_id uuid PRIMARY KEY,
    input_version text NOT NULL,
    as_of timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    payload jsonb NOT NULL,
    UNIQUE (input_version, as_of)
);
CREATE TABLE opportunities (
    opportunity_id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES instruments,
    feature_snapshot_id uuid REFERENCES feature_snapshots,
    regime_snapshot_id uuid REFERENCES regime_snapshots,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    observed_at timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    payload jsonb NOT NULL
);
CREATE TABLE decisions (
    decision_id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES instruments,
    side text NOT NULL CHECK (side IN ('LONG','SHORT','NO_TRADE')),
    state text NOT NULL CHECK (state IN ('DISCOVERED','WATCHING','ARMED','VALIDATING',
        'APPROVED','REJECTED','EXPIRED','NO_TRADE','CANCELLED','QUEUED','EXECUTING','MANAGING','CLOSED')),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    observed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    snapshot text NOT NULL CHECK (jsonb_typeof(snapshot::jsonb) = 'object'),
    snapshot_hash text NOT NULL CHECK (
        snapshot_hash = encode(sha256(convert_to(snapshot, 'UTF8')), 'hex'))
);
CREATE INDEX decisions_instrument_time ON decisions (instrument_id, observed_at);
CREATE TABLE decision_reasons (
    reason_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    decision_id uuid NOT NULL REFERENCES decisions,
    reason_code text NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (decision_id, reason_code)
);
CREATE TABLE risk_snapshots (
    snapshot_id uuid PRIMARY KEY,
    as_of timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    payload jsonb NOT NULL
);
CREATE TABLE execution_intents (
    intent_id uuid PRIMARY KEY,
    client_order_id varchar(36) NOT NULL UNIQUE CHECK (client_order_id ~ '^[A-Za-z0-9._:/-]+$'),
    client_sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    decision_id uuid NOT NULL REFERENCES decisions,
    instrument_id uuid NOT NULL REFERENCES instruments,
    risk_snapshot_id uuid NOT NULL REFERENCES risk_snapshots,
    side text NOT NULL CHECK (side IN ('LONG','SHORT')),
    qty numeric NOT NULL CHECK (finite_numeric(qty) AND qty > 0),
    price numeric CHECK (finite_numeric(price) AND price > 0),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at),
    version bigint NOT NULL DEFAULT 0 CHECK (version = 0),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    snapshot jsonb NOT NULL,
    UNIQUE (intent_id, client_order_id)
);
CREATE INDEX intents_decision ON execution_intents (decision_id);
CREATE FUNCTION validate_intent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM decisions d WHERE d.decision_id = NEW.decision_id
        AND d.state = 'APPROVED' AND d.instrument_id = NEW.instrument_id
        AND d.side = NEW.side AND d.expires_at >= NEW.expires_at
        AND d.expires_at > CURRENT_TIMESTAMP AND NEW.expires_at > CURRENT_TIMESTAMP) THEN
        RAISE EXCEPTION 'INVALID_APPROVED_DECISION' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER approved_intent BEFORE INSERT ON execution_intents
FOR EACH ROW EXECUTE FUNCTION validate_intent();
CREATE TABLE outbox (
    message_id uuid PRIMARY KEY,
    intent_id uuid NOT NULL UNIQUE REFERENCES execution_intents,
    topic text NOT NULL CHECK (topic = 'execution.intent'),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivered_at timestamptz,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1)
);
CREATE INDEX outbox_pending ON outbox (created_at, message_id) WHERE delivered_at IS NULL;
CREATE FUNCTION immutable_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.message_id <> OLD.message_id OR NEW.intent_id <> OLD.intent_id
       OR NEW.topic <> OLD.topic OR NEW.payload <> OLD.payload
       OR NEW.created_at <> OLD.created_at OR NEW.schema_version <> OLD.schema_version
       OR OLD.delivered_at IS NOT NULL OR NEW.delivered_at IS NULL THEN
        RAISE EXCEPTION 'IMMUTABLE_OUTBOX_MESSAGE' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER immutable_outbox_message BEFORE UPDATE ON outbox
FOR EACH ROW EXECUTE FUNCTION immutable_outbox();
CREATE TABLE orders (
    intent_id uuid PRIMARY KEY REFERENCES execution_intents,
    client_order_id varchar(36) NOT NULL UNIQUE REFERENCES execution_intents(client_order_id),
    exchange_order_id text UNIQUE,
    status text NOT NULL DEFAULT 'NEW' CHECK (status IN ('NEW','ACK_UNKNOWN','ACCEPTED',
        'PARTIALLY_FILLED','FILLED','CANCELED','EXPIRED','REJECTED','UNKNOWN')),
    qty numeric NOT NULL CHECK (finite_numeric(qty) AND qty > 0),
    filled_qty numeric NOT NULL DEFAULT 0 CHECK (
        finite_numeric(filled_qty) AND filled_qty >= 0 AND filled_qty <= qty),
    avg_price numeric CHECK (finite_numeric(avg_price) AND avg_price > 0),
    fee numeric NOT NULL DEFAULT 0 CHECK (finite_numeric(fee)),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    FOREIGN KEY (intent_id, client_order_id)
        REFERENCES execution_intents (intent_id, client_order_id)
);
CREATE TABLE fills (
    fill_id text PRIMARY KEY,
    intent_id uuid NOT NULL REFERENCES orders,
    qty numeric NOT NULL CHECK (finite_numeric(qty) AND qty > 0),
    price numeric NOT NULL CHECK (finite_numeric(price) AND price > 0),
    fee numeric NOT NULL CHECK (finite_numeric(fee)),
    exchange_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL CHECK (observed_at >= exchange_at),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1)
);
CREATE INDEX fills_intent ON fills (intent_id);
CREATE TABLE positions (
    position_id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES instruments,
    side text NOT NULL CHECK (side IN ('LONG','SHORT')),
    qty numeric NOT NULL CHECK (finite_numeric(qty) AND qty >= 0),
    entry_price numeric NOT NULL CHECK (finite_numeric(entry_price) AND entry_price > 0),
    mark_price numeric NOT NULL CHECK (finite_numeric(mark_price) AND mark_price > 0),
    protected_qty numeric NOT NULL DEFAULT 0 CHECK (
        finite_numeric(protected_qty) AND protected_qty >= 0 AND protected_qty <= qty),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    as_of timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    UNIQUE (instrument_id, side)
);
CREATE TABLE protective_orders (
    protective_id uuid PRIMARY KEY,
    position_id uuid NOT NULL REFERENCES positions,
    client_order_id varchar(36) NOT NULL UNIQUE,
    exchange_order_id text UNIQUE,
    qty numeric NOT NULL CHECK (finite_numeric(qty) AND qty > 0),
    stop_price numeric NOT NULL CHECK (finite_numeric(stop_price) AND stop_price > 0),
    status text NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    as_of timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1)
);
CREATE TABLE account_snapshots (
    snapshot_id uuid PRIMARY KEY,
    as_of timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    payload jsonb NOT NULL
);
CREATE TABLE risk_state (
    state_id text PRIMARY KEY,
    risk_snapshot_id uuid NOT NULL REFERENCES risk_snapshots,
    kill_switch boolean NOT NULL DEFAULT true,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    as_of timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1)
);
CREATE TABLE commands (
    command_id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    command_type text NOT NULL,
    payload jsonb NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    observed_at timestamptz NOT NULL,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1)
);
CREATE TABLE audit_log (
    audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1)
);
CREATE TABLE consumer_receipts (
    consumer text NOT NULL,
    message_id uuid NOT NULL REFERENCES outbox,
    consumed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer, message_id)
);

DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['instruments','universe_snapshots','closed_bars',
        'feature_snapshots','regime_snapshots','decisions','decision_reasons',
        'execution_intents','risk_snapshots','account_snapshots','audit_log','consumer_receipts']
    LOOP
        EXECUTE format('CREATE TRIGGER append_only_rows BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION append_only()', t);
        EXECUTE format('CREATE TRIGGER append_only_truncate BEFORE TRUNCATE ON %I '
            'FOR EACH STATEMENT EXECUTE FUNCTION append_only()', t);
    END LOOP;
    FOREACH t IN ARRAY ARRAY['opportunities','outbox','orders','positions','protective_orders',
                             'risk_state','commands']
    LOOP
        EXECUTE format('CREATE TRIGGER monotonic_version BEFORE UPDATE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION version_successor()', t);
    END LOOP;
END $$;
