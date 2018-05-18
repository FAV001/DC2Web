BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS `versionporg` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`version`	TEXT UNIQUE
);
INSERT INTO `versionporg` (id,version) VALUES (1,'21.34.04 06.04.07'),
 (2,'21.36.44 20.04.07'),
 (3,'21.40.02 10.08.07'),
 (4,'21.40.04 03.09.07'),
 (5,'21.40.04 10.08.07'),
 (6,'21.40.04 28.01.09'),
 (7,'21.40.44 10.08.07'),
 (8,'21.40.44 28.01.09'),
 (9,'23.40.85 31.01.17'),
 (10,'29.40.44 28.01.07'),
 (11,NULL);
CREATE TABLE IF NOT EXISTS `type` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`name`	TEXT,
	`comment`	TEXT
);
INSERT INTO `type` (id,name,comment) VALUES (1,'ПР','проводной'),
 (2,'РУ','радиоудлинитель'),
 (3,'СП','спутник'),
 (4,'GSM','GSM таксофон');
CREATE TABLE IF NOT EXISTS `serviceman` (
	`id`	INTEGER UNIQUE,
	`name`	TEXT UNIQUE,
	`use`	INTEGER DEFAULT 0
);
INSERT INTO `serviceman` (id,name,use) VALUES (1,'Монтеры',1),
 (4,'Белогорский Район',1),
 (5,'Серышевский Район',1),
 (9,'Архаринский Район',1),
 (10,'Михайловский Район',1),
 (11,'Завитой',1),
 (13,'Бурейский Район',1),
 (16,'Свободненский Район',1),
 (21,'Баташов Н.П.',1),
 (23,'Шимановский Район',1),
 (26,'Райчихинск',1),
 (33,'Мазановский Район',1),
 (35,'Ромненский Район',1),
 (38,'Тамбовский',1),
 (41,'Тамбовский Район',1),
 (42,'Ивановский Район',1),
 (43,'тест',1),
 (45,'Тындинский Район',1),
 (46,'Константиновский Район',1),
 (52,'Февральскй',1),
 (53,'Селемджинский Район',1),
 (54,'Благовещенский Район',1),
 (55,'Завитинский Район',1),
 (56,'Зейский Район',1),
 (57,'Магдагачинский Район',1),
 (58,'Октябрьский Район',1),
 (59,'Сковородинский Район',1),
 (60,'Благовещенск Город',1);
CREATE TABLE IF NOT EXISTS `region` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`name`	TEXT
);
INSERT INTO `region` (id,name) VALUES (1,'Амурская обл');
CREATE TABLE IF NOT EXISTS `phone` (
	`id`	INTEGER,
	`address`	TEXT,
	`fias`	TEXT,
	`region_id`	INTEGER,
	`district_id`	INTEGER,
	`note`	TEXT,
	`slot_id`	INTEGER,
	`work_sam`	INTEGER,
	`versionprog_id`	INTEGER,
	`lot_numb`	TEXT,
	`competition_numb`	TEXT,
	`abc_numb`	TEXT,
	`line_numb`	TEXT,
	PRIMARY KEY(`id`)
);
CREATE TABLE IF NOT EXISTS `district` (
	`id`	INTEGER UNIQUE,
	`name`	TEXT UNIQUE,
	`use`	INTEGER DEFAULT 0
);
INSERT INTO `district` (id,name,use) VALUES (1,'Районы',1),
 (10,'Архара',1),
 (12,'Завитинск',1),
 (13,'Екатеринославка',1),
 (14,'Новобурейск',1),
 (23,'Райчихинск',1),
 (45,'УУС Райчих-к гор. окр.',1),
 (46,'УУС Прогресс гор. окр.',1),
 (49,'УУС Констант-й муний.р-н',1),
 (50,'УУС Иван-й муниц. р-н',1),
 (51,'УУС Ромненский муниц.р-н',1),
 (52,'УУС Михайл-й муниц. р-н',1),
 (53,'УУС Белогорск гор.окр.',1),
 (54,'УУС Свободный гор.окр.',1),
 (55,'УУС Углегорск гор. окр.',1),
 (56,'УУС Белог-й муниц.р-н',1),
 (57,'УУС Мазан-й муниц.р-н',1),
 (58,'УУС Серышев-й муниц.р-н',1),
 (59,'УУС Свободн-й муниц.р-н',1),
 (60,'УУС Селемдж-й муниц.р-н',1),
 (61,'УУС Шимановск гор. окр.',1),
 (62,'УУС Шиман-й муниц.р-н',1),
 (64,'тамбовка',1),
 (66,'Благовещенск',1),
 (68,'тест',1),
 (69,'КОН 181 ЛОТ 1',1),
 (73,'Снятые',1),
 (75,'УУС Завитинский р-н',1),
 (76,'УУС Зейский р-н',1),
 (77,'УУС Магдагачинский р-н',1),
 (78,'УУС Октябрьский р-н',1),
 (79,'УУС Сковородинский р-н',1),
 (80,'УУС Тамбовский р-н',1),
 (81,'УУС Тындинский р-н',1),
 (82,'УУС Благовещенск гор',1),
 (83,'УУС Благовещенский р-н',1),
 (84,'УУС Бурейский р-н',1),
 (85,'УУС Архаринский р-н',1);
CREATE TABLE IF NOT EXISTS `AREA` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`region`	INTEGER,
	`name`	TEXT
);
INSERT INTO `AREA` (id,region,name) VALUES (1,1,'Архаринский Район'),
 (2,1,'Белогорск Город'),
 (3,1,'Белогорский Район'),
 (4,1,'Благовещенск Город'),
 (5,1,'Благовещенский Район'),
 (6,1,'Бурейский Район'),
 (7,1,'Завитинский Район'),
 (8,1,'Зейский Район'),
 (9,1,'Зея Город'),
 (10,1,'Ивановский Район'),
 (11,1,'Константиновский Район'),
 (12,1,'Магдагачи Район'),
 (13,1,'Магдагачинский Район'),
 (14,1,'Мазановский Район'),
 (15,1,'Михайловский Район'),
 (16,1,'Октябрьский Район'),
 (17,1,'Прогресс Поселок городского типа'),
 (18,1,'Райчихинск Город'),
 (19,1,'Ромненский Район'),
 (20,1,'Свободненский Район'),
 (21,1,'Свободный Город'),
 (22,1,'Селемджинский Район'),
 (23,1,'Серышевский Район'),
 (24,1,'Сковородинский Район'),
 (25,1,'Тамбовский Район'),
 (26,1,'Тында Город'),
 (27,1,'Тындинский Район'),
 (28,1,'Углегорск Поселок'),
 (29,1,'Шимановск Город'),
 (30,1,'Шимановский Район');
CREATE UNIQUE INDEX IF NOT EXISTS `phone_id` ON `phone` (
	`id`	DESC
);
COMMIT;
