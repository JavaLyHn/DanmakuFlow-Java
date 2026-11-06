-- MIT License, Copyright (c) 2026 0x.LLyHn; see repository LICENSE.
-- MySQL 8.4 target. Operator must explicitly select a NEW database before migrating.
-- Database creation, clean, baseline and modification of legacy tables are not performed.
CREATE TABLE danmaku_messages (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id BINARY(16) NOT NULL,
    room_id VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    content VARCHAR(500) CHARACTER SET utf8mb4 NOT NULL,
    send_time DATETIME(3) NOT NULL COMMENT 'UTC, millisecond precision',
    PRIMARY KEY (id),
    UNIQUE KEY uk_danmaku_event_id (event_id),
    KEY idx_room_time_id (room_id, send_time, id),
    KEY idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
