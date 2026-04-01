# Continuous Intelligence

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy the example and create your version
- **Glossary** - project terms and concepts

## Additional Resources

- [Suggested Datasets](https://denisecase.github.io/pro-analytics-02/reference/datasets/cintel/)


## Custom Project

### Dataset
The dataset used in this project contains time-series system metrics. Each row represents an observation at a specific timestamp and includes the number of requests, number of errors, and total latency in milliseconds. This data helps analyze how system performance changes over time.

### Signals
I used the rolling mean signals from the example, including requests_rolling_mean, errors_rolling_mean, and latency_rolling_mean. In addition, I created a new signal called requests_rolling_std, which calculates the rolling standard deviation of the requests column. This new signal helps measure how much the request values vary over time.

### Experiments
I modified the pipeline by adding a rolling standard deviation calculation for the requests column. This experiment was done to go beyond just averages and include variability in the analysis. I kept the same window size but added a new expression using rolling_std to generate an additional signal.

### Results
After running the modified pipeline, a new column called requests_rolling_std was added to the output file. This column shows how much the request values fluctuate within each window. When the data is stable, the values are low, and when there are changes or spikes, the values increase.

### Interpretation
This modification provides deeper insight into system behavior. While the rolling mean shows the general trend, the rolling standard deviation highlights variability and potential instability. This can help detect unusual patterns, sudden spikes, or inconsistent performance, which is important for monitoring and decision-making in continuous systems.
