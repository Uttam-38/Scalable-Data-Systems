DROP TABLE IF EXISTS query1 CASCADE;
DROP TABLE IF EXISTS query2 CASCADE;
DROP TABLE IF EXISTS query3 CASCADE;
DROP TABLE IF EXISTS query4 CASCADE;
DROP TABLE IF EXISTS query5 CASCADE;

-- Query 1: 
CREATE TABLE query1 AS
SELECT COUNT(*) AS "count of comments"
FROM comments
WHERE author = 'xymemez';


-- Query 2: 
CREATE TABLE query2 AS
SELECT subreddit_type AS "subreddit type",
       COUNT(*) AS "subreddit count"
FROM subreddits
GROUP BY subreddit_type;


-- Query 3: 
CREATE TABLE query3 AS
SELECT c.subreddit AS name,
       COUNT(c.id) AS "comments count",
       ROUND(AVG(c.score)::NUMERIC, 2) AS "average score"
FROM comments c
GROUP BY c.subreddit
ORDER BY COUNT(c.id) DESC
LIMIT 10;


-- Query 4: 
CREATE TABLE query4 AS
SELECT name,
       COALESCE(link_karma,0) AS "link karma",
       COALESCE(comment_karma,0) AS "comment karma",
       CASE WHEN COALESCE(link_karma,0) >= COALESCE(comment_karma,0) THEN 1 ELSE 0 END AS label
FROM authors
WHERE ((COALESCE(link_karma,0) + COALESCE(comment_karma,0)) / 2.0) > 1000000
ORDER BY ((COALESCE(link_karma,0) + COALESCE(comment_karma,0)) / 2.0) DESC, name;


-- Query 5: 
CREATE TABLE query5 AS
SELECT s.subreddit_type AS "sr type",
       COUNT(c.id) AS "comments num"
FROM comments c
LEFT JOIN subreddits s
  ON c.subreddit_id = s.name
WHERE c.author = '[deleted_user]'
GROUP BY s.subreddit_type;
