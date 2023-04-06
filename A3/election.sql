drop schema if exists election cascade;
create schema election;
set search_path to election;

-- ASSUMPTIONS:
-- 1. The database only contains information about only the latest election.
-- 2. There can be any number of candidates in this one election.
-- 3. Each candidate can only have one election campaign.


-- Table that is a global list of all Humans involved in the election
-- Each human is uniquely identified by their e-mail
CREATE TABLE Human (
	email VARCHAR(50) PRIMARY KEY
);

-- Table that identifies each candidate and their campaign
-- Also contains the budget of their campaign
CREATE TABLE Candidate (
	email VARCHAR(50) PRIMARY KEY REFERENCES Human(email),
	budget INTEGER NOT NULL
);

-- INSERT INTO Human (email, role) VALUES ('abc', 'candidate');
-- INSERT INTO Human (email, role) VALUES ('bca', 'volunteer');
-- INSERT INTO ElectionCampaign VALUES (1, 100);
-- INSERT INTO Candidate VALUES (1, 1);

CREATE TYPE CampaignActivity AS ENUM ('phone banks', 'door-to-door canvassing');

-- Enumeration to define the types of Humans in election apart from Candidates
CREATE TYPE RoleType AS ENUM ('volunteer', 'staff');

CREATE TABLE Worker (
	email INTEGER NOT NULL ,
	-- email INTEGER NOT NULL REFERENCES HumanExceptCandidate(email),
	works_for VARCHAR(50) NOT NULL REFERENCES Candidate(email),
	UNIQUE (email, works_for),
	role RoleType NOT NULL,

	activity_time TIMESTAMP NOT NULL,
	CHECK (EXTRACT(minute FROM activity_time) = 0 
		AND EXTRACT(second FROM activity_time) = 0),
	UNIQUE (email, activity_time),
	
	activity CampaignActivity NOT NULL
);

CREATE TABLE Debate (
	id INTEGER,
	moderator_id INTEGER NOT NULL,
	start_time TIMESTAMP NOT NULL,
	end_time TIMESTAMP NOT NULL,

	PRIMARY KEY(id, campaign_id),
	CHECK (start_time > end_time)
);

CREATE TABLE DebateCandidate (
	debate_id INTEGER NOT NULL,
	candidate_id INTEGER NOT NULL,

	UNIQUE(debate_id, candidate_id)
);

CREATE TABLE Donor (
	id INTEGER PRIMARY KEY,
	address varchar(256) NOT NULL,
	is_individual BOOLEAN NOT NULL
);

CREATE TABLE Donation (
	donor_id INTEGER,
	campaign_id INTEGER REFERENCES ElectionCampaign(id),
	time TIMESTAMP NOT NULL,
	donation_amount INTEGER NOT NULL,

	UNIQUE (donor_id, campaign_id, time)
);
