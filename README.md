# Network-Analysis---Detect-Communities
Implementing the Girvan-Newman algorithm to detect communities within a distributed environment in an efficient way using the Spark Framework in Python.
A sub-dataset, ub_sample_data.csv,from the Yelp review dataset containing user_id and business_id, to test the program.
Constructed a social network graph : each node represents a user, an edge exists between two nodes if the number of common businesses reviewed by two users is greater than or equivalent to the filter threshold.
In task1: constructing the social network graph and then using the Spark GraphFrames library to detect communities in the network graph constructed from the dataset.
In task2: implemented the Girvan-Newman algorithm to detect the communities in the network graph. Calculated the betweenness of each edge in the original graph i constructed and then divided the graph into suitable communities, which reaches the global highest modularity.
