DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS submissions CASCADE;
DROP TABLE IF EXISTS subreddits CASCADE;
DROP TABLE IF EXISTS authors CASCADE;

CREATE TABLE authors (
    id TEXT NULL,
    retrieved_on BIGINT NULL,
    name TEXT PRIMARY KEY,
    created_utc BIGINT NULL,
    link_karma INTEGER NULL,
    comment_karma INTEGER NULL,
    profile_img TEXT NULL,
    profile_color TEXT NULL,
    profile_over_18 BOOLEAN NULL
);


CREATE TABLE subreddits (
    banner_background_image TEXT NULL,
    created_utc BIGINT NULL,
    description TEXT NULL,
    display_name TEXT UNIQUE,
    header_img TEXT NULL,
    hide_ads BOOLEAN NULL,
    id TEXT NULL,
    over18 BOOLEAN NULL,
    public_description TEXT NULL,
    retrieved_utc BIGINT NULL,
    name TEXT PRIMARY KEY,
    subreddit_type TEXT NULL,
    subscribers INTEGER NULL,
    title TEXT NULL,
    whitelist_status TEXT NULL
);

CREATE TABLE submissions (
    downs INTEGER NULL,
    url TEXT NULL,
    id TEXT PRIMARY KEY,
    edited BOOLEAN NULL,
    num_reports INTEGER NULL,
    created_utc BIGINT NULL,
    name TEXT NULL,
    title TEXT NULL,
    author TEXT NULL,
    permalink TEXT NULL,
    num_comments INTEGER NULL,
    likes BOOLEAN NULL,
    subreddit_id TEXT NULL,
    ups INTEGER NULL
);

CREATE TABLE comments (
    distinguished TEXT NULL,
    downs INTEGER NULL,
    created_utc BIGINT NULL,
    controversiality INTEGER NULL,
    edited BOOLEAN NULL,
    gilded INTEGER NULL,
    author_flair_css_class TEXT NULL,
    id TEXT PRIMARY KEY,
    author TEXT NULL,
    retrieved_on BIGINT NULL,
    score_hidden BOOLEAN NULL,
    subreddit_id TEXT NULL,
    score INTEGER NULL,
    name TEXT NULL,
    author_flair_text TEXT NULL,
    link_id TEXT NULL,
    archived BOOLEAN NULL,
    ups INTEGER NULL,
    parent_id TEXT NULL,
    subreddit TEXT NULL,
    body TEXT NULL
);
