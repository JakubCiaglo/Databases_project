CREATE TABLE clients
(
  client_id     INT          NOT NULL AUTO_INCREMENT,
  first_name    VARCHAR(50)  NOT NULL,
  last_name     VARCHAR(50)  NOT NULL,
  date_of_birth DATE         NULL,
  email         VARCHAR(100) NULL,
  phone         VARCHAR(30)  NULL,
  PRIMARY KEY (client_id)
);

ALTER TABLE clients
  ADD CONSTRAINT UQ_email UNIQUE (email);

CREATE TABLE costs
(
  cost_id     INT           NOT NULL AUTO_INCREMENT,
  trip_id     INT           NOT NULL,
  description VARCHAR(255)  NOT NULL,
  cost_amount DECIMAL(12,2) NOT NULL,
  PRIMARY KEY (cost_id)
);

CREATE TABLE destinations
(
  destination_id INT          NOT NULL AUTO_INCREMENT,
  name           VARCHAR(100) NOT NULL,
  description    TEXT         NULL,
  avg_gravity    DECIMAL(5,2) NULL,
  hazard_level   VARCHAR(20)  NULL,
  PRIMARY KEY (destination_id)
);

ALTER TABLE destinations
  ADD CONSTRAINT UQ_name UNIQUE (name);

CREATE TABLE emergency_contacts
(
  contact_id   INT          NOT NULL AUTO_INCREMENT,
  client_id    INT          NOT NULL,
  first_name   VARCHAR(50)  NOT NULL,
  last_name    VARCHAR(50)  NOT NULL,
  relationship VARCHAR(50)  NULL,
  email        VARCHAR(100) NULL,
  phone        VARCHAR(30)  NULL,
  PRIMARY KEY (contact_id)
);

CREATE TABLE employees
(
  employee_id      INT           NOT NULL AUTO_INCREMENT,
  first_name       VARCHAR(50)   NOT NULL,
  last_name        VARCHAR(50)   NOT NULL,
  position         VARCHAR(100)  NOT NULL,
  salary           DECIMAL(10,2) NOT NULL,
  hire_date        DATE          NOT NULL,
  termination_date DATE          NULL,
  email            VARCHAR(100)  NULL,
  phone            VARCHAR(30)   NULL,
  PRIMARY KEY (employee_id)
);

ALTER TABLE employees
  ADD CONSTRAINT UQ_email UNIQUE (email);

CREATE TABLE feedback
(
  feedback_id  INT      NOT NULL AUTO_INCREMENT,
  trip_id      INT      NOT NULL,
  client_id    INT      NOT NULL,
  rating       TINYINT  NULL,
  comments     TEXT     NULL,
  submitted_at DATETIME NULL,
  PRIMARY KEY (feedback_id)
);

CREATE TABLE incidents
(
  incident_id          INT         NOT NULL AUTO_INCREMENT,
  trip_id              INT         NOT NULL,
  datetime_occurred    DATETIME    NOT NULL,
  reported_by_employee INT         NULL,
  involved_client_id   INT         NULL,
  category             VARCHAR(50) NULL,
  description          TEXT        NOT NULL,
  severity             VARCHAR(20) NULL,
  PRIMARY KEY (incident_id)
);

CREATE TABLE launch_stations
(
  launch_station_id INT          NOT NULL AUTO_INCREMENT,
  name              VARCHAR(100) NOT NULL,
  country           VARCHAR(100) NOT NULL,
  city              VARCHAR(100) NULL,
  status            VARCHAR(20)  NULL,
  PRIMARY KEY (launch_station_id)
);

ALTER TABLE launch_stations
  ADD CONSTRAINT UQ_name UNIQUE (name);

CREATE TABLE rockets
(
  rocket_id    INT          NOT NULL AUTO_INCREMENT,
  name         VARCHAR(100) NOT NULL,
  manufacturer VARCHAR(100) NULL,
  status       VARCHAR(20)  NULL,
  PRIMARY KEY (rocket_id)
);

ALTER TABLE rockets
  ADD CONSTRAINT UQ_name UNIQUE (name);

CREATE TABLE spacecraft
(
  spacecraft_id       INT          NOT NULL AUTO_INCREMENT,
  capacity_passengers INT          NOT NULL,
  name                VARCHAR(100) NOT NULL,
  manufacturer        VARCHAR(100) NULL,
  service_start_date  DATE         NOT NULL,
  service_end_date    DATE         NULL,
  status              VARCHAR(20)  NULL,
  PRIMARY KEY (spacecraft_id)
);

ALTER TABLE spacecraft
  ADD CONSTRAINT UQ_name UNIQUE (name);

CREATE TABLE transactions
(
  transaction_id   INT           NOT NULL AUTO_INCREMENT,
  trip_id          INT           NOT NULL,
  client_id        INT           NOT NULL,
  transaction_date DATETIME      NOT NULL,
  amount           DECIMAL(12,2) NOT NULL,
  payment_method   VARCHAR(50)   NOT NULL,
  status           VARCHAR(20)   NOT NULL,
  PRIMARY KEY (transaction_id)
);

CREATE TABLE trip_types
(
  trip_type_id          INT           NOT NULL AUTO_INCREMENT,
  name                  VARCHAR(100)  NOT NULL,
  description           TEXT          NULL,
  typical_duration_days INT           NULL,
  base_price            DECIMAL(12,2) NOT NULL,
  PRIMARY KEY (trip_type_id)
);

ALTER TABLE trip_types
  ADD CONSTRAINT UQ_name UNIQUE (name);

CREATE TABLE trips
(
  trip_id            INT         NOT NULL AUTO_INCREMENT,
  trip_type_id       INT         NOT NULL,
  destination_id     INT         NOT NULL,
  launch_station_id  INT         NOT NULL,
  spacecraft_id      INT         NOT NULL,
  rocket_id          INT         NULL,
  departure_datetime DATETIME    NOT NULL,
  return_datetime    DATETIME    NULL,
  status             VARCHAR(20) NOT NULL,
  PRIMARY KEY (trip_id)
);

CREATE TABLE employee_assignments
(
  assignment_id INT NOT NULL AUTO_INCREMENT,
  trip_id       INT NOT NULL,
  employee_id   INT NOT NULL,
  PRIMARY KEY (assignment_id),
  UNIQUE (trip_id, employee_id)
);

CREATE TABLE trip_participants
(
  participation_id INT NOT NULL AUTO_INCREMENT,
  trip_id          INT NOT NULL,
  client_id        INT NOT NULL,
  seat_number      INT NULL,
  PRIMARY KEY (participation_id),
  UNIQUE (trip_id, client_id)
);

ALTER TABLE costs
  ADD CONSTRAINT FK_trips_TO_costs
    FOREIGN KEY (trip_id)
    REFERENCES trips (trip_id);

ALTER TABLE emergency_contacts
  ADD CONSTRAINT FK_clients_TO_emergency_contacts
    FOREIGN KEY (client_id)
    REFERENCES clients (client_id);

ALTER TABLE employee_assignments
  ADD CONSTRAINT FK_trips_TO_employee_assignments
    FOREIGN KEY (trip_id)
    REFERENCES trips (trip_id);

ALTER TABLE employee_assignments
  ADD CONSTRAINT FK_employees_TO_employee_assignments
    FOREIGN KEY (employee_id)
    REFERENCES employees (employee_id);

ALTER TABLE feedback
  ADD CONSTRAINT FK_trips_TO_feedback
    FOREIGN KEY (trip_id)
    REFERENCES trips (trip_id);

ALTER TABLE feedback
  ADD CONSTRAINT FK_clients_TO_feedback
    FOREIGN KEY (client_id)
    REFERENCES clients (client_id);

ALTER TABLE incidents
  ADD CONSTRAINT FK_trips_TO_incidents
    FOREIGN KEY (trip_id)
    REFERENCES trips (trip_id);

ALTER TABLE incidents
  ADD CONSTRAINT FK_employees_TO_incidents
    FOREIGN KEY (reported_by_employee)
    REFERENCES employees (employee_id);

ALTER TABLE incidents
  ADD CONSTRAINT FK_clients_TO_incidents
    FOREIGN KEY (involved_client_id)
    REFERENCES clients (client_id);

ALTER TABLE transactions
  ADD CONSTRAINT FK_trips_TO_transactions
    FOREIGN KEY (trip_id)
    REFERENCES trips (trip_id);

ALTER TABLE transactions
  ADD CONSTRAINT FK_clients_TO_transactions
    FOREIGN KEY (client_id)
    REFERENCES clients (client_id);

ALTER TABLE trip_participants
  ADD CONSTRAINT FK_trips_TO_trip_participants
    FOREIGN KEY (trip_id)
    REFERENCES trips (trip_id);

ALTER TABLE trip_participants
  ADD CONSTRAINT FK_clients_TO_trip_participants
    FOREIGN KEY (client_id)
    REFERENCES clients (client_id);

ALTER TABLE trips
  ADD CONSTRAINT FK_trip_types_TO_trips
    FOREIGN KEY (trip_type_id)
    REFERENCES trip_types (trip_type_id);

ALTER TABLE trips
  ADD CONSTRAINT FK_destinations_TO_trips
    FOREIGN KEY (destination_id)
    REFERENCES destinations (destination_id);

ALTER TABLE trips
  ADD CONSTRAINT FK_launch_stations_TO_trips
    FOREIGN KEY (launch_station_id)
    REFERENCES launch_stations (launch_station_id);

ALTER TABLE trips
  ADD CONSTRAINT FK_spacecraft_TO_trips
    FOREIGN KEY (spacecraft_id)
    REFERENCES spacecraft (spacecraft_id);

ALTER TABLE trips
  ADD CONSTRAINT FK_rockets_TO_trips
    FOREIGN KEY (rocket_id)
    REFERENCES rockets (rocket_id);