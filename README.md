# RStudio Addin for Submitting R Jobs to Databricks

## Getting Started

Install the `databricksRJobSubmit` addin from [tnederlof/databricksRJobSubmit](https://github.com/tnederlof/databricksRJobSubmit):

```
install.packages("remotes")
remotes::install_github("tnederlof/databricksRJobSubmit")
```
Start a Databricks all-purpose compute cluster that support running R code.

Ensure this cluster either has the appropriate R packages pre-installed or place package install commands at the start of each R script for installation at run time (follow the instructions below to speed up package installs).

Cluster setup instructions to speed up R package installs:
1. Use [https://packagemanager.posit.co/client/#/repos/cran/setup?r_environment=other](https://packagemanager.posit.co/client/#/repos/cran/setup?r_environment=other) to get the relevant repository URL for your setup (be mindful that Ubuntu versions change between DBR versions). For example the URL for Ubuntu 22.04 is https://packagemanager.posit.co/cran/__linux__/jammy/latest.
2. Create an environment variable at the cluster level named DATABRICKS_DEFAULT_R_REPOS, containing the repo URL found in step 1 as the value.
3. Create an init script at the cluster level to set the HTTPUserAgent in Rprofile.site. For example:
```
#!/bin/bash
# Append changes to Rprofile.site
cat <<EOF >> "/etc/R/Rprofile.site"

options(
  HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(), paste(getRversion(), R.version["platform"], R.version["arch"], R.version["os"]))
)

EOF
```

## Usage

Ensure your RStudio Pro session running in Workbench has [Databricks credentials enabled](https://docs.posit.co/ide/server-pro/user/posit-workbench/managed-credentials/databricks.html).

Select "Submit R Script" in the "Addins" menu:  
![image](https://github.com/user-attachments/assets/806243fa-560a-4ec4-b267-a581841f2a1f)

Enter the two required fields ("Local R file path:" and "Databricks cluster id:") and click "Submit", a link to the job in Databricks will be displayed as a URL in the console. R packages only need to be installed once on the cluster, if they have been previously installed there is no need to enter the package names each job submit request.
