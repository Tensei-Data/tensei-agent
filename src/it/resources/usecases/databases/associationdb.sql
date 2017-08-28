CREATE TABLE IF NOT EXISTS accounts (
  id number(10) NOT NULL,
  name varchar(150) DEFAULT NULL,
  vorname varchar(200) NOT NULL,
  date_entered datetime DEFAULT NULL,
  birthday date DEFAULT NULL,
  description text,
  deleted number(1) DEFAULT '0',
  phone_office varchar(100) DEFAULT NULL,
  website varchar(255) DEFAULT NULL
);

--
-- Daten für Tabelle `accounts`
--

INSERT INTO accounts VALUES(1, 'Mustermann', 'Max', '2015-09-07 00:00:00', '2013-05-21', NULL, 0, '017612345678', NULL);
INSERT INTO accounts VALUES(2, 'Berentzen', '', '2015-03-09 12:00:00', '2013-01-24', 'Das ist eine Beschreibung', 1, '011134567890', 'http://www.seineseite.de');
INSERT INTO accounts VALUES(3, 'Musterfrau', 'Frauke', '2015-03-09 15:00:00', '2012-03-08', 'Sie hat auch eine Beschreibung.', 1, NULL, 'http://www.fraukemusterfrau.com');
INSERT INTO accounts VALUES(4, 'Musterkind', '', '2015-09-08 03:23:12', '2013-05-14', NULL, 0, NULL, NULL);
INSERT INTO accounts VALUES(5, 'Boss', 'Hugo', '2014-04-18 14:22:51', '2016-01-03', NULL, 0, '0987654321', 'http://www.hugoboss.de');
INSERT INTO accounts VALUES(6, 'Münteraner', 'Ölaf', '2016-05-12 15:29:20', '2013-05-24', '---------', 0, NULL, NULL);

-- INSERT INTO accounts VALUES(1, 'Mustermann', 'Max', '2015-09-07 00:00:00', '2013-05-24', NULL, 0, '017612345678', NULL);
-- INSERT INTO accounts VALUES(2, 'Berentzen', '', '2015-03-09 12:00:00', NULL, 'Das ist eine Beschreibung', 1, '011134567890', 'http://www.seineseite.de');
-- INSERT INTO accounts VALUES(3, 'Musterfrau', 'Frauke', NULL, '2012-03-08', 'Sie hat auch eine Beschreibung.', 1, NULL, 'http://www.fraukemusterfrau.com');
-- INSERT INTO accounts VALUES(4, 'Musterkind', '', '2015-09-08 03:23:12', NULL, NULL, 0, NULL, NULL);
-- INSERT INTO accounts VALUES(5, 'Boss', 'Hugo', '2014-04-18 14:22:51', '2016-01-03', NULL, 0, '0987654321', 'http://www.hugoboss.de');
-- INSERT INTO accounts VALUES(6, 'Münteraner', 'Ölaf', '2016-05-12 15:29:20', NULL, '---------', 0, NULL, NULL);