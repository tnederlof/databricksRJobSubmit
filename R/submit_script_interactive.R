#' Submit an R Script
#'
#' Transform an R script into a notebook task, installs R packages if needed,
#' and submits as job to a Databricks cluster.
#' @import shiny
#' @import fs
#' @import miniUI
#' @import brickster
#' @import rstudioapi
#' @import cli
#'
#' @export
submit_script_interactive <- function() {
  ui <- miniUI::miniPage(
    miniUI::gadgetTitleBar("Submit R Script to Databricks"),
    miniUI::miniContentPanel(
      miniUI::miniContentPanel(
        shiny::textInput("input_path", "Local R file path:"),
        shiny::textInput("cluster_id", "Databricks cluster id:"),
        shiny::textInput("databricks_path", "Databricks Workspace folder (optional):"),
        shiny::textInput("run_name", "Job name (optional):"),
        shiny::textInput("r_packages", "R packages to install (comma delimited, optional):"),
        shiny::actionButton("submit", "Submit Job")
      )
    )
  )

  server <- function(input, output, session) {

    shiny::observeEvent(input$submit, {

      if (input$r_packages == "") {
        r_packages <- NULL
      } else {
        r_packages <- strsplit(gsub(" ", "", input$r_packages, fixed = TRUE), ",")[[1]]
      }
      if (input$run_name == "") {
        run_name <- NULL
      } else {
        run_name <- input$run_name
      }

      result <- submit_script(input$input_path, input$cluster_id, input$databricks_path,
                              r_packages, run_name)

      # look up job details
      token <- workbench_databricks_token(Sys.getenv("DATABRICKS_HOST"), Sys.getenv("DATABRICKS_CONFIG_FILE"))
      job_details <- db_jobs_runs_get(result$run_id, token = token)
      job_url <- job_details$run_page_url
      cli::cli_text("Databricks job started. Visit: {.url {job_url}}.")


      shiny::stopApp()
    })

  }

  shiny::runGadget(ui, server, viewer = shiny::dialogViewer("Submit R Job to Databrick", width = 1000, height = 800))
}


submit_script <- function(input_path, cluster_id, databricks_path = NULL, r_packages = NULL, run_name = NULL) {
  # check env variables
  if (Sys.getenv("DATABRICKS_HOST") == "") {
    stop("Env variable DATABRICKS_HOST not found. Ensure your session is started with Databricks credentials: https://docs.posit.co/ide/server-pro/user/posit-workbench/managed-credentials/databricks.html")
  }
  if (Sys.getenv("DATABRICKS_CONFIG_FILE") == "") {
    stop("Env variable DATABRICKS_CONFIG_FILE not found. Ensure your session is started with Databricks credentials: https://docs.posit.co/ide/server-pro/user/posit-workbench/managed-credentials/databricks.html")
  }

  # check input_path
  input_path_split_vec <- fs::path_split(input_path)[[1]]
  input_filename_and_ext <- strsplit(input_path_split_vec[[length(input_path_split_vec)]], split ='\\.')[[1]]
  input_filename <- input_filename_and_ext[[1]]
  input_ext <- input_filename_and_ext[[2]]
  if (tolower(input_ext) != "r") {
    stop("A problem occured with the input path, please ensure its a file ending in 'r' or 'R'")
  }

  token <- workbench_databricks_token(Sys.getenv("DATABRICKS_HOST"), Sys.getenv("DATABRICKS_CONFIG_FILE"))
  timestamp_chr <- as.character(as.numeric(as.POSIXct(Sys.time())) * 100000)

  # check databricks_path
  current_user_info <- brickster::db_current_user(Sys.getenv("DATABRICKS_HOST"), token)
  if (!is.null(databricks_path) & !(databricks_path == "")) {
    db_path_split_vec <- fs::path_split(databricks_path)[[1]]
    if ((db_path_split_vec[[2]] != "Workspace") | (db_path_split_vec[[3]] != "Users") | (db_path_split_vec[[4]] != current_user_info$userName)) {
      stop(paste0("The Databricks workspace path must start with the format '/Workspace/Users/", current_user_info$userName, "/'"))
    }
  } else {
    databricks_path <- do.call(fs::path, list("/Workspace", "Users", current_user_info$userName))
  }


  temp_dir_path <- fs::path_temp()
  new_filename <- paste0(input_filename, "_databricks_notebook_", timestamp_chr, ".R")
  new_filename_no_ext <- paste0(input_filename, "_databricks_notebook_", timestamp_chr)
  temp_path <- do.call(fs::path, list(temp_dir_path, new_filename))
  suppressWarnings(convert_r_notebook(input_path, temp_path))

  databricks_full_path <- do.call(fs::path, list(databricks_path, new_filename))

  import_result <- tryCatch(
    {
      return_code <- brickster::db_workspace_import(
        path = databricks_full_path,
        file = temp_path,
        format = "SOURCE",
        language = "R",
        overwrite = T,
        host = Sys.getenv("DATABRICKS_HOST"),
        token = token
      )
      return_code
    },
    error = function(cond) {
      stop(paste("Error uploading notebook to the workspace: ", cond))
    },
    warning = function(cond) {
      stop(paste("Warning uploading notebook to the workspace: ", cond))
    }
  )

  if (is.null(run_name)) {
    run_name_used <- paste("Workbench Submitted Job", timestamp_chr)
  } else {
    run_name_used <- run_name
  }

  notebook_task <- brickster::job_task(
    task_key = timestamp_chr,
    existing_cluster_id = cluster_id,
    task = brickster::notebook_task(notebook_path = databricks_full_path)
  )



  # install R packages on the cluster, script will block until they finish
  if (!is.null(r_packages)) {
    install_results <- install_r_packages(cluster_id, r_packages, token = token)
  }

  job_result <- tryCatch(
    {
      job_submit_result <- brickster::db_jobs_runs_submit(
        tasks = brickster::job_tasks(notebook_task),
        run_name = run_name_used,
        idempotency_token = timestamp_chr,
        token=token
      )
      job_submit_result
    },
    error = function(cond) {
      stop(paste("Error submitting the job: ", cond))
    },
    warning = function(cond) {
      stop(paste("Warning submitting the job: ", cond))
    }
  )
  job_result
}


install_r_packages <- function(cluster_id, r_packages, repo_url = "https://packagemanager.posit.co/cran/__linux__/jammy/latest", token = token) {
  lapply(r_packages, function(x) install_r_package(x, repo_url, cluster_id = cluster_id, token = token))
}



install_r_package <- function(package, repo_url, cluster_id, token) {
  brickster::db_libs_install(
    cluster_id = cluster_id,
    libraries = brickster::libraries(
      brickster::lib_cran(package = package, repo = repo_url)
    ),
    host = Sys.getenv("DATABRICKS_HOST"),
    token = token
  )
}

workbench_databricks_token <- function(host, cfg_file) {
  cfg <- readLines(cfg_file)
  # We don't attempt a full parse of the INI syntax supported by Databricks
  # config files, instead relying on the fact that this particular file will
  # always contain only one section.
  if (!any(grepl(host, cfg, fixed = TRUE))) {
    # The configuration doesn't actually apply to this host.
    return(NULL)
  }
  line <- grepl("token = ", cfg, fixed = TRUE)
  token <- gsub("token = ", "", cfg[line])
  if (nchar(token) == 0) {
    return(NULL)
  }
  token
}


convert_r_notebook <- function(input_filename, output_filename) {
  code <- readLines(input_filename)
  final_code <- c(c("# Databricks notebook source", "# COMMAND ----------"), code)
  writeLines(final_code, file(output_filename))
}
