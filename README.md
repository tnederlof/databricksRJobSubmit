# RStudio Addin for Submitting R Jobs to Databricks

## Getting Started

Install the `databricksRJobSubmit` addin from [tnederlof/databricksRJobSubmit](https://github.com/tnederlof/databricksRJobSubmit):

```
install.packages("remotes")
remotes::install_github("tnederlof/databricksRJobSubmit")
```
Start a Databricks all-purpose compute cluster that support running R code. Ensure this cluster has the appropriate R packages installed as needed by your script (optionally the addin has a space for new packages that need to be installed).

## Usage

Ensure your RStudio Pro session running in Workbench has [Databricks credentials enabled](https://docs.posit.co/ide/server-pro/user/posit-workbench/managed-credentials/databricks.html).

Select "Submit R Script" in the "Addins" menu:
![image](https://github.com/user-attachments/assets/806243fa-560a-4ec4-b267-a581841f2a1f)

Enter the required fields and click "Submit", a link to the job in Databricks will be displayed as a URL in the console. R packages only need to be installed once on the cluster, if they have been previously installed there is no need to enter the package names each job submit request.
