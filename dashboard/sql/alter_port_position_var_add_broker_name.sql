-- Migration: rename broker_name to broker in port_position_var
-- broker_account already exists; this renames the companion column for consistency with position_var.
ALTER TABLE port_position_var
    RENAME COLUMN broker_name TO broker;
