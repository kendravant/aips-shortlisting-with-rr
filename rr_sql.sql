-- At the very start we want to restrict ourselves to only looking at jobs for which we have human outcomes

-- Ensure that each job in our training set has at least 2 shortlisted and at least two rejected applications
SELECT * INTO MISSandBox.kv.multi_outcome_jobs
FROM (
  SELECT
    SQ1.ListingID,
    SQ1.NumRejected,
    SQ2.NumShortlisted
  FROM
    (SELECT
       ListingID,
       COUNT(*) AS NumRejected
     FROM MISSandBox.kv.applications_reviewed_with_outcomes
     WHERE Outcome = 'Rejected'
     GROUP BY ListingID
    ) AS SQ1
    JOIN
    (SELECT
       ListingID,
       COUNT(*) AS NumShortlisted
     FROM MISSandBox.kv.applications_reviewed_with_outcomes
     WHERE Outcome = 'Shortlisted'
     GROUP BY ListingID
    ) AS SQ2
      ON SQ1.ListingID = SQ2.ListingID
  WHERE NumShortlisted > 1 AND NumRejected > 1
     ) AS SQ3

-- Extract the jobs which have role requirements questions and multi-outcomes
select
     JobID,
     ListingID,
     IndustryID,
     DisciplineID,
     WorkTypeID,
     LocationID,
     IsEnhancedAd,
     CreateDate,
     StatusID,
     LinkOut
from MISSandBox.kv.nac_jobs_info
WHERE HasQuestionnaireUrl = 1
AND ListingID in (select DISTINCT ListingID from MISSandBox.kv.multi_outcome_jobs WHERE NumRejected > 9 and NumShortlisted > 9)
-- Record count 97,835


-- Pick up all the AcceptedApplicationID so we can link to the RR data
-- Look only at the set of jobs with at least 2 shortlisted and at least 2 rejected applications
-- Look only at jobs using RR
SELECT
  SQ1.ProspectID,
  SQ1.ListingID,
  SQ1.Outcome,
  SQ1.NumOutcomeApps,
  SQ1.NumRejected,
  SQ1.NumShortlisted,
  SQ2.AcceptedApplicationId
from
    (SELECT
       A.*,
       B.NumRejected,
       B.NumShortlisted
        FROM MISSandBox.kv.applications_reviewed_with_outcomes AS A
        JOIN MISSandBox.kv.multi_outcome_jobs AS B
            ON A.ListingID = B.ListingID) AS SQ1
  JOIN
    (SELECT
       A.ApplicationCorrelationID,
       A.ApplicationID,
       A.AcceptedApplicationId,
       B.ProspectID,
       B.ListingID
     FROM MISData.odsvw.WEB_dbo_ApplicationCorrelation A
       JOIN MISSandBox.kv.applications_summary B
         ON A.AcceptedApplicationId = B.FatFeedApplicationID
     WHERE [$ETL_EffectiveYN] = 'Y') AS SQ2
  ON SQ1.ProspectID = SQ2.ProspectID
WHERE SQ1.Outcome in ('Rejected','Shortlisted')
        AND SQ1.ListingID in (SELECT DISTINCT ListingID
                              FROM MISSandBox.kv.nac_jobs_info
                              WHERE HasQuestionnaireUrl = 1)


-- There are both shortlisted and rejected outcomes in the RR jobs
SELECT Outcome, count(*) as cnt
FROM MISSandBox.kv.applications_reviewed_with_outcomes
WHERE NumOutcomeApps > 5
      AND Outcome in ('Rejected','Shortlisted')
      AND ListingID in (SELECT DISTINCT ListingID
                        FROM MISSandBox.kv.nac_jobs_info
                        WHERE HasQuestionnaireUrl = 1)
GROUP BY Outcome


-- RR jobs with 2 or more shortlisted and rejected outcomes (from MIS)
select count(DISTINCT job_id) from sandbox.kendra_nac_jobs
-- 297,714

-- Select the subset of jobs where
-- we have recorded the selected preferred answer set
-- AND the RR questions are not custom
--
--DROP TABLE sandbox.kendra_questions
CREATE TABLE sandbox.kendra_questions
AS
(
SELECT job_id,
       question_id,
       question_text,
       question_option_metadata,
       COUNT(*) AS num_records
FROM dataplatform_jobs.enriched_jobs_jobquestionnaire
 WHERE job_id IN (SELECT job_id
                      FROM sandbox.kendra_nac_jobs
                      WHERE job_id IS NOT NULL)
       AND question_option_metadata IS NOT NULL
       AND length(question_id) < 30
  GROUP BY job_id, question_id, question_text, question_option_metadata
)
--------------------------------------------------------------
-- Jobs with standard questions and preferred answers
select count(DISTINCT job_id) from sandbox.kendra_questions
-- 252,039
-- Questions with answer preference set captured
select count(*) from sandbox.kendra_questions
-- 964,054
select count(DISTINCT question_id) FROM sandbox.kendra_questions
-- 952
--------------------------------------------------------------

-- Unnest the questions and cast the preferred answer indicator to a binary 0/1
-- DROP TABLE sandbox.kendra_questions_unnested
CREATE TABLE sandbox.kendra_questions_unnested
AS
(
SELECT
  job_id,
  question_id,
  question_text,
  question_option_metadata,
  answer_options.option AS answer_option,
  cast(answer_options.ispreferred AS INTEGER) AS is_preferred_answer
FROM sandbox.kendra_questions
CROSS JOIN unnest (question_option_metadata) as T(answer_options)
)


-- Pick up the answers of all the applicant for the jobs selected above
-- DROP TABLE sandbox.kendra_nested_answers
CREATE TABLE sandbox.kendra_nested_answers
AS
(
SELECT
  advertisement_id AS job_id,
  application_id,
  candidate_id,
  question_id,
  answer,
  COUNT(*) AS num_records
FROM dataplatform_apply.native_apply_questionnaire_answers
  WHERE advertisement_id IN (SELECT DISTINCT job_id
                             FROM sandbox.kendra_questions)
       AND length(question_id) < 30
  GROUP BY candidate_id, application_id, advertisement_id, question_id, answer
  -- Record count 71,655,098
)

select count(distinct job_id) from sandbox.kendra_nested_answers
-- 252,039

-- Subset to only the candidates for whom we have application outcomes
-- Unnest the candidate answers (this creates many rows in the case of a multi select answer)
-- DROP TABLE sandbox.kendra_answers_with_outcomes
CREATE TABLE sandbox.kendra_answers_with_outcomes
AS
(
  select
    SQ1.job_id,
    SQ1.application_id,
    SQ1.candidate_id,
    SQ1.question_id,
    candidate_answer,
    SQ1.outcome,
    SQ1.num_outcome_apps
  FROM
    (
    select
      B.job_id,
      B.application_id,
      B.candidate_id,
      B.question_id,
      B.answer,
      A.outcome,
      A.num_outcome_apps
    from sandbox.kendra_application_outcomes A
      join sandbox.kendra_nested_answers B
      on A.accepted_application_id = B.application_id
      where listing_id is not null
      -- record count is 32,580,771
    ) AS SQ1
  CROSS JOIN unnest (answer) AS T(candidate_answer)
)

select count(distinct job_id) from sandbox.kendra_answers_with_outcomes
-- 252,039

-- Each question and each candidate answer (exploded for multi-select questions) with flag to say whether the answer is from preferred set
-- Also has application outcomes attached
-- DROP TABLE sandbox.kendra_question_and_answer
CREATE TABLE sandbox.kendra_question_and_answer
AS
(
SELECT A.job_id, A.question_id, B.application_id, A.question_text, A.question_option_metadata,
       B.candidate_answer, A.is_preferred_answer, B.outcome
  FROM sandbox.kendra_questions_unnested AS A
  JOIN sandbox.kendra_answers_with_outcomes AS B
  ON A.job_id = B.job_id
  AND A.question_id = B.question_id
  AND A.answer_option = B.candidate_answer
  -- record count  is 39,362,259
)

select count(distinct job_id) from sandbox.kendra_question_and_answer
-- 252,039

-- The number of preferred answers for each {job_id, question_id} pair
-- Used to determine whether a candidates answer is a Yes, No or Partial match to the preferred answer set
-- DROP TABLE sandbox.kendra_num_preferred_answers
CREATE TABLE sandbox.kendra_num_preferred_answers
AS
(
select job_id, question_id, question_text, count(*) AS num_answers, sum(is_preferred_answer) AS num_pref_answers
from sandbox.kendra_questions_unnested
GROUP BY job_id, question_id, question_text
-- record count is 964,038
)

select * from sandbox.kendra_num_preferred_answers order by num_pref_answers ASC

-- Build the feature vectors for each application (anywhere from 1 to 5 features per application)
-- The triplet {question_id, question_option_metadata, ynp} is the feature
-- We need to remove the jobs for which there were questions with no preferred answers otherwise we get divide by zero problems
-- DROP TABLE sandbox.kendra_ad_features
CREATE TABLE sandbox.kendra_ad_features
AS
(
SELECT A.application_id, A.job_id, A.question_id, A.question_option_metadata, --B.question_text,
  COALESCE(
  CASE WHEN (B.num_pref_answers > 0 AND A.pref_sum = B.num_pref_answers) THEN 'Y' ELSE NULL END,
  CASE WHEN (B.num_pref_answers > 0 AND A.pref_sum > 0 AND A.pref_sum < B.num_pref_answers) THEN 'P' ELSE NULL END,
  CASE WHEN (B.num_pref_answers > 0 AND A.pref_sum = 0) THEN 'N' ELSE NULL END,
  CASE WHEN (B.num_pref_answers = 0) THEN 'P' ELSE NULL END -- Currently counting the special case of no preferred answers specified as a partial match for all candidates
  ) AS ynp
FROM
  (SELECT
    job_id,
    question_id,
    application_id,
    question_option_metadata,
    sum(is_preferred_answer) AS pref_sum
  FROM sandbox.kendra_question_and_answer
  GROUP BY application_id, job_id, question_id, question_option_metadata
  ) AS A
JOIN
  sandbox.kendra_num_preferred_answers AS B
  ON A.job_id = B.job_id
  AND A.question_id = B.question_id
  -- record count is 32,581,534
)

-- Create distinct feature set
--DROP TABLE sandbox.kendra_feature_set
CREATE TABLE sandbox.kendra_full_feature_set
AS
(
  select 100000 + row_number() OVER (ORDER BY question_id ASC, question_option_metadata ASC) AS feature_id,
         question_id,
         question_option_metadata,
         ynp,
         num_counts
  FROM
  (
    SELECT question_id,
           question_option_metadata,
           ynp,
           COUNT(*) AS num_counts
    FROM sandbox.kendra_ad_features
    GROUP BY question_id, question_option_metadata, ynp
    -- record count is 25,921
  )
)

CREATE TABLE sandbox.kendra_ynp_feature_set
AS
(
  select 200000 + row_number() OVER (ORDER BY question_id ASC) AS feature_id,
         question_id,
         ynp,
         num_counts
  FROM
  (
    SELECT question_id,
           ynp,
           COUNT(*) AS num_counts
    FROM sandbox.kendra_ad_features
    GROUP BY question_id, ynp
    -- record count is 2,495
  )
)

select * from sandbox.kendra_ynp_feature_set
order by feature_id ASC

select count(distinct question_id) from sandbox.kendra_full_feature_set
--order by feature_id ASC

select question_id, count(*) as variants
from sandbox.kendra_feature_set
group by question_id
order by variants DESC

select count(*) from sandbox.kendra_ynp_feature_set
where num_counts > 10

-- Answers per application
-- Not sure how we get anything greater than 5 but the numbers are very small
select num_features, count(*) as occ
from
(
select application_id, count(*) AS num_features
from (
select A.application_id, A.job_id, B.feature_id
from sandbox.kendra_ad_features A join sandbox.kendra_ynp_feature_set B
ON A.question_id = B.question_id
AND A.ynp = B.ynp
)
group by application_id
)
group by num_features
order by num_features DESC


-- Answers per job
-- Makes sense to get up to 15 features (3 x 5 questions)
-- A bit strange that we have 5,673 jobs with only one exercised answer to a single question - but actually it might just be cut down by our candidate set (only those shortlisted and rejected)
SELECT num_features, count(*) as occ
FROM
(
  select job_id, count(*) AS num_features
  from
  (
    select job_id, feature_id, count(*) as num_applicant
    from
    (
      select A.application_id, A.job_id, B.feature_id, A.question_id, A.ynp
      from sandbox.kendra_ad_features A join sandbox.kendra_ynp_feature_set B
      ON A.question_id = B.question_id
      AND A.ynp = B.ynp
    )
  group by job_id, feature_id
  )
group by job_id
)
group by num_features
order by num_features DESC

-- Add the outcomes to the tall ynp feature table
CREATE TABLE sandbox.kendra_ynp_training_data
AS
(
SELECT A.application_id, A.job_id, B.prospect_id, B.listing_id, B.outcome, A.feature_id
FROM
  (
  select A.application_id, A.job_id, B.feature_id
  from sandbox.kendra_ad_features A join sandbox.kendra_ynp_feature_set B
   ON A.question_id = B.question_id
   AND A.ynp = B.ynp
  ) AS A
JOIN sandbox.kendra_application_outcomes AS B
ON A.application_id = B.accepted_application_id
)

select * from sandbox.kendra_ynp_training_data
