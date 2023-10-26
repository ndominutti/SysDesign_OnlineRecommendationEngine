# SysDesign_OnlineRecommendationEngine

This repository contains the implementation of a web-ads recommendation engine ML system. The process runs every day at midnight, generating recommendations for each user, storing them in AWS RDS, and retrieving them when a user enters the web.

The processes involved are:

- ETL & Recommendation (code stored in /dags)
- Recommendations Serving (code stored in /api)

For a more in-depth understanding of the purpose and the technology employed, I recommend visiting this [Medium post](https://ndominutti.medium.com/ml-systems-a-practical-introduction-798dbdfe4a16), which provides a detailed explanation.

