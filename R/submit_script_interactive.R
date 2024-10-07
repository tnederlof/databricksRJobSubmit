#' Submit an R Script
#'
#' Transform an R script into a notebook task, installs R packages if needed,
#' and submits as job to a Databricks cluster.
#'
#' @export
submit_script_interactive <- function() {
  ui <- miniPage(

    includeHighlightJs(),
    gadgetTitleBar("Submit R Script to Databricks"),
    miniContentPanel(
      h4("Replace the text 'from' with the text 'to'."),
      hr(),
      stableColumnLayout(
        textInput("input_path", "Local R file path:"),
        textInput("databricks_path", "Databricks Workspace folder:"),
        textInput("cluster_id", "Databricks cluster id:")
      ),
      stableColumnLayout(
        textInput("r_packages", "R packages to install (optional):"),
        textInput("run_name", "Job run name (optional):"),
        textInput("task_key", "Task key name (optional):"),
        textInput("description", "Task description (optional:")
      ),
      actionButton("submit", "Submit Job"),
    )
  )

  server <- function(input, output, session) {

    observeEvent(submit$done, {

      if (r_packages == "") {
        r_packages <- NULL
      }
      if (run_name == "") {
        run_name <- NULL
      }
      if (task_key == "") {
        task_key <- NULL
      }
      if (description == "") {
        description <- NULL
      }

      submit_script(input$input_path, input$databricks_path, input$cluster_id,
                    r_packages, run_name, task_key, description)

      stopApp()
    })

  }

  viewer <- dialogViewer("Submit R Job to Databrick", width = 1000, height = 800)
  runGadget(ui, server, viewer = viewer)
}


submit_script <- function(input_path, databricks_path, cluster_id, r_packages = NULL, run_name = NULL, task_key = NULL, description = NULL, overwrite = F) {
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
  db_path_split_vec <- fs::path_split(databricks_path)[[1]]
  current_user_info <- brickster::db_current_user(Sys.getenv("DATABRICKS_HOST"), token)
  if ((db_path_split_vec[[2]] != "Workspace") | (db_path_split_vec[[3]] != "Users") | (db_path_split_vec[[4]] != current_user_info$userName)) {
    stop(paste0("The Databricks workspace path must start with the format '/Workspace/Users/", current_user_info$userName, "/'"))
  }

  temp_dir_path <- fs::path_temp()
  new_filename <- paste0(input_filename, "_databricks_notebook_", timestamp_chr, ".R")
  new_filename_no_ext <- paste0(input_filename, "_databricks_notebook_", timestamp_chr)
  temp_path <- do.call(fs::path, list(temp_dir_path, new_filename))
  print(temp_path)
  suppressWarnings(convert_r_notebook(input_path, temp_path))

  databricks_full_path <- do.call(fs::path, list(databricks_path, new_filename))

  import_result <- tryCatch(
    {
      return_code <- db_workspace_import(
        path = databricks_full_path,
        file = temp_path,
        format = "SOURCE",
        language = "R",
        overwrite = overwrite,
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

  if (is.null(task_key)) {
    task_key <- new_filename_no_ext
  }

  notebook_task <- job_task(
    task_key = task_key,
    description = description,
    existing_cluster_id = cluster_id,
    task = notebook_task(notebook_path = databricks_full_path)
  )

  if (is.null(run_name)) {
    run_name <- paste("Workbench Submitted Job", timestamp_chr)
  }

  # install R packages on the cluster, script will block until they finish
  if (!is.null(r_packages)) {
    install_r_packages(cluster_id, r_packages)
  }

  job_result <- tryCatch(
    {
      job_submit_result <- db_jobs_runs_submit(
        tasks = job_tasks(notebook_task),
        run_name = run_name,
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
  install_result <- lapply(r_packages, function(x) install_r_package(x, repo_url, token))
  return(wait_until_packages_done(cluster_id, r_packages, token = token))
}



wait_until_packages_done <- function(cluster_id, r_packages, polling_interval = 1, token) {
  lib_statuses <- "PENDING"
  while (any(lib_statuses != "INSTALLED")) {
    lib_query <- db_libs_cluster_status(cluster_id = cluster_id, token = token)
    relevant_library_status <- purrr::keep(lib_query$library_statuses, \(x) x[[1]][[1]]$package %in% r_packages)
    lib_statuses <- purrr::map_chr(relevant_library_status, "status")
    lib_names <- purrr::map_chr(relevant_library_status, \(x) x[[1]][[1]]$package)

    if ("FAILED" %in% lib_statuses) {
      stop(paste("The following libraries failed to install:", paste(lib_names[lib_statuses == "FAILED"], collapse = ", ")))
    }
    Sys.sleep(polling_interval)
  }
  NULL
}



if (all(lib_statuses == "INSTALLED")) {
  return ()
}



install_r_package <- function(package, repo_url, token) {
  db_libs_install(
    cluster_id = cluster_id,
    libraries = libraries(
      lib_cran(package = package, repo = repo_url)
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
