ALTER TABLE submissions
ADD CONSTRAINT fk_submissions_author
FOREIGN KEY (author)
REFERENCES authors(name);

ALTER TABLE submissions
ADD CONSTRAINT fk_submissions_subreddit
FOREIGN KEY (subreddit_id)
REFERENCES subreddits(name);

ALTER TABLE comments
ADD CONSTRAINT fk_comments_author
FOREIGN KEY (author)
REFERENCES authors(name);

ALTER TABLE comments
ADD CONSTRAINT fk_comments_subreddit
FOREIGN KEY (subreddit)
REFERENCES subreddits(display_name);

ALTER TABLE comments
ADD CONSTRAINT fk_comments_subreddit_id
FOREIGN KEY (subreddit_id)
REFERENCES subreddits(name);
