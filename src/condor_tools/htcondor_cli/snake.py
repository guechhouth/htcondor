import htcondor2 as htcondor
import shutil
import sys
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from htcondor_cli.noun import Noun
from htcondor_cli.verb import Verb
import traceback
import re
import argparse
import os

from htcondor_cli.noun import Noun
from htcondor_cli.verb import Verb
from htcondor2._utils.ansi import Color, colorize

class Submit(Verb):
    """
    Submit a local universe job and Snakemake jobs when run.
    """
    # command-line argument configurations
    options = {
        "jobdir": {
            "args": ("--jobdir",),
            "help": "Optional directory for HTCondor management job log files. If omitted, a `logs` directory will be created at the current directory. Must be specified before the -- separator.",
            "required": False,
        },
        "snakemake_args": {
            "args": ("snakemake_args",),
            "nargs": argparse.REMAINDER,
            "help": "Snakefile followed by optional snakemake arguments. Usage: [--jobdir DIR] [Snakefile] [-- snakemake_args]. Snakefile and --jobdir must come before the -- separator.",
        }
    }

    def __init__(self, logger, snakefile=None, snakemake_args=None, **options):
        """
        Initialize and submit a Snakemake management job.

        This constructor performs CLI-style parsing of the provided arguments,
        validates inputs, prepares the job directory, and submits a local
        universe HTCondor job which will run Snakemake with the HTCondor
        executor.

        Args:
            logger: Logger object used for logging messages.
            snakefile (str or pathlib.Path, optional): Path to the Snakefile to run.
            snakemake_args (list[str], optional): Remaining arguments intended
                for Snakemake (may include a leading "--" separator or jobdir).
            **options: Additional options parsed from the CLI. Supported key:
                - "jobdir": path to a directory where management logs are written.

        Returns:
            None

        Raises:
            FileNotFoundError: if the resolved Snakefile does not exist.
            RuntimeError: if Snakemake executable cannot be found or submission fails.
        """
        if snakefile is None:
            # Extract --jobdir from snakemake_args if present
            snakemake_args = self._extract_jobdir_from_remainder(snakemake_args, options)
            if snakemake_args and snakemake_args[0] == '--':
                # When no snakefile given, we can just strip the separator
                snakemake_args = snakemake_args[1:]
            else:
                if snakemake_args and not snakemake_args[0].startswith('-'):
                    snakefile = snakemake_args.pop(0)
                # Strip the -- separator that may follow the snakefile
                if snakemake_args and snakemake_args[0] == '--':
                    snakemake_args = snakemake_args[1:]

        # Basic validations of CLI
        snakefile = self._validate_snakefile(snakefile)
        jobdir = self._setup_jobdir(options)

        # submit a local universe job
        try:
            self._submit_local(snakefile, jobdir, snakemake_args)
        except Exception as e:
            print("Error: Could not submit local universe job.")
            print(f"Details:", str(e))
            sys.exit(1)
        
    def _extract_jobdir_from_remainder(self, snakemake_args, options):
        """
        Extract any `--jobdir` occurrences from the remainder `snakemake_args`.

        Only `--jobdir` occurrences appear before the separator `--` are considered. 
        If multiple `--jobdir` flags are present the last value wins and is written into
        `options['jobdir']`.

        Args:
            snakemake_args (list[str] or None): The argument list from the CLI
                that will be forwarded to Snakemake. May include a leading
                "--" separator and `--jobdir`.
            options (dict): Mutable dict of parsed options. Will be updated
                with a "jobdir" key if a `--jobdir` value is found.

        Returns:
            list[str] or None: A filtered list of `snakemake_args` with any
            `--jobdir` flags and their values removed. Returns the original
            `snakemake_args` if no `--jobdir` was found.
        """
        if not snakemake_args:
            return snakemake_args
        
        # Find the position of -- separator
        separator_index = None
        try:
            separator_index = snakemake_args.index('--')
        except ValueError:
            pass  # No separator found
        
        # Scan and extract all --jobdir occurrences from before the separator
        scan_range = separator_index if separator_index is not None else len(snakemake_args)
        
        # Collect all --jobdir values (last one will be used)
        jobdir_value_in_remainder = None
        indices_to_skip = set()
        
        i = 0
        while i < len(snakemake_args):
            if i < scan_range and snakemake_args[i] == '--jobdir' and i + 1 < scan_range:
                # Found --jobdir before the separator
                # Unintended behavior if value of --jobdir is not specified
                jobdir_value_in_remainder = snakemake_args[i + 1]  # Update to latest value
                indices_to_skip.add(i)      # Mark --jobdir for removal
                indices_to_skip.add(i + 1)  # Mark its value for removal
                i += 2
            else:
                i += 1
        
        # If we found any --jobdir in REMAINDER, rebuild filtered args
        if indices_to_skip:
            options["jobdir"] = jobdir_value_in_remainder
            filtered_args = []
            for i in range(len(snakemake_args)):
                if i not in indices_to_skip:
                    filtered_args.append(snakemake_args[i])
            return filtered_args
        
        return snakemake_args

    def _validate_snakefile(self, snakefile):
        """
        Validate and normalize the given snakefile path.

        If `snakefile` is `None`, this function defaults to the string
        "Snakefile" and verifies the file exists.

        Args:
            snakefile (str or path or None): Path or name of the Snakefile.

        Returns:
            pathlib.Path: Resolved Path object pointing to the Snakefile.

        Raises:
            FileNotFoundError: If the resolved path does not exist.
        """
        # if snakefile is not provided
        if snakefile is None:
            snakefile = "Snakefile"
        snakefile = Path(snakefile)
        if not snakefile.exists():
            raise FileNotFoundError(
                f"Could not find Snakefile: {snakefile}\n"
                f"Make sure to provide the path to the Snakefile or place it at the submit directory or "
            )
        return snakefile

    def _setup_jobdir(self, options):
        """
        Create or resolve the job directory for management logs.

        Args:
            options (dict): Parsed CLI options. If the dict contains a
                "jobdir" key its value will be used as the job directory.

        Returns:
            pathlib.Path: Path to the created or existing job directory.
        """
        if options.get("jobdir"):
            jobdir = Path(options.get("jobdir"))
        else:
            jobdir = Path.cwd() / "logs"

        jobdir.mkdir(parents=True, exist_ok=True)
        return jobdir

    # ===== HTCondor SUBMISSION METHODS ===== #
    def _submit_local(self, snakefile, jobdir, snakemake_args):
        """
        Submit Snakemake as an HTCondor local-universe management job.

        This method discovers the Snakemake executable, constructs a
        Submit description for HTCondor, submits the job, writes a pointer
        file into `.snakemake/htcondor` so other commands (eg. `status`)
        can locate the workflow job directory, and prints submission info.

        Args:
            snakefile (pathlib.Path or str): Path to the Snakefile to run.
            jobdir (pathlib.Path): Directory where management logs should be
                written and where metadata will be stored.
            snakemake_args (list[str] or None): Extra arguments to forward to
                Snakemake when it runs.

        Returns:
            None

        Raises:
            RuntimeError: If the Snakemake executable cannot be found.
            Exception: Any exception raised by HTCondor submission is
                propagated to the caller.
        """
        # Resolve snakemake executable from user's environment
        snakemake_path = shutil.which("snakemake")
        if snakemake_path is None:
            raise RuntimeError(
                "Could not find 'snakemake' executable on PATH.\n"
                "Make sure your Snakemake environment is activated."
            )

        # Build arguments for snakemake
        args_list = [
            f"-s {snakefile}",
            f"--executor htcondor",
            f"--htcondor-jobdir {jobdir}"
        ]
        
        # Add any additional snakemake args
        if snakemake_args:
            args_list.extend(snakemake_args)
        
        arguments = " ".join(args_list)
        
        submit_description = htcondor.Submit({
            "executable": snakemake_path,
            "arguments": arguments,
            "universe": "local",
            "request_disk": "512MB",
            "request_cpus": 1,
            "request_memory": 512,

            # Set up logging
            "log": f"{jobdir}/snakemake-mgmt-$(ClusterId).log",
            "output": f"{jobdir}/snakemake-mgmt-$(ClusterId).out",
            "error": f"{jobdir}/snakemake-mgmt-$(ClusterId).err",

            # Specify getenv so the job uses the submitter's environment
            "getenv": "true",

            # Inject mgmt_id at submit time so executor can read it immediately
            # $(ClusterId) is expanded by HTCondor before the job starts, avoiding
            # the race condition of schedd.edit() after submission
            "environment": "SNAKEMAKE_MGMT_ID=$(ClusterId)",

            "JobBatchName": "snakemake-mgmt-$(ClusterId)",
        })

        # Submit to HTCondor
        schedd = htcondor.Schedd()
        submit_result = schedd.submit(submit_description)

        cluster_id = submit_result.cluster()

        # Write a pointer so `htcondor snake status <mgmt_id` can find the right jobdir
        # Pointer is at original htcondor log path (./snakemake/htcondor)        
        pointer = {"jobdir": str(jobdir.resolve())}
        pointer_dir = Path(".snakemake/htcondor")
        pointer_dir.mkdir(parents=True, exist_ok=True)
        pointer_path = pointer_dir / f"snakemake-htcondor-{cluster_id}.json"
        pointer_path.write_text(json.dumps(pointer))

        print(f"Snakemake management job submitted with JobID {cluster_id}.0")
        print(f"Logs can be found in {jobdir}")

class Status(Verb):
    """
    Shows the current status of a workflow when given the management's ID.
    Reads from cached metadata file instead of querying schedd for efficiency.
    Jobdir is auto-discovered from metadata.
    """
    # Command-line argument configurations
    options = {
        "mgmt_id": {
            "args": ("mgmt_id",), # positional argument
            "help": "Positional argument for a management JobID that oversees the entire workflow. Must be specified.",
        },
    }

    def __init__(self, logger, mgmt_id=None, **options):
        """
        Initialize a status viewer and display workflow status.

        This constructor locates the pointer file written at submit time to
        discover the job directory, reads cached metadata, and prints a
        user-friendly status summary. It avoids expensive schedd queries by
        using the cached metadata file.

        Args:
            logger: Logger instance for logging messages.
            mgmt_id (str or int): Management job ClusterId for the workflow.
            **options: Reserved for future options.

        Returns:
            None

        Raises:
            FileNotFoundError: If the pointer or metadata file cannot be found.
        """
        self.logger = logger

        if mgmt_id is None:
            print("Error: management ID is required")
            return
        
        try:
            # Find the jobdir via pointer file written at submit time
            current_directory = os.getcwd()
            pointer_path = Path(f"{current_directory}/.snakemake/htcondor/snakemake-htcondor-{mgmt_id}.json")
            if not pointer_path.exists():
                print(f"Error: No workflow pointer found for job {mgmt_id}")
                print("Make sure you run 'htcondor snake status' from the same directory as 'htcondor snake submit'.")
                return
            
            with open(pointer_path) as f:
                pointer = json.load(f)
            
            # Get jobdir and find metadata path
            jobdir = Path(pointer["jobdir"])
            metadata_path = jobdir/ f"snakemake-metadata-{mgmt_id}.json"

            if not metadata_path.exists():
                print(f"Error: Metadata not found at {metadata_path}")
                print("Make sure you run from the directory where 'htcondor snake submit' was executed.")
                return
            
            with open(metadata_path) as f:
                metadata = json.load(f)

            self._show_status(mgmt_id, metadata)
            
        except Exception as e:
            print(f"Error: Could not get status for job {mgmt_id}")
            print(f"Exception: {str(e)}")
            traceback.print_exc()
    
    def _show_status(self, mgmt_id, metadata):
        """
        Read cached metadata and produce a human-readable status display.

        Args:
            mgmt_id (str or int): Management ClusterId used to label the
                workflow in output.
            metadata (dict): Parsed JSON metadata containing job status and
                DAG information produced by Snakemake.

        Returns:
            None
        """
        # Query schedd only for management job info (elapsed time, status)
        mgmt_job_ad = self._get_mgmt_job_info(mgmt_id)
        
        # Display using metadata for job statuses (efficient, no schedd queries)
        self._display_workflow_status(mgmt_id, metadata, mgmt_job_ad)
    
    def _get_mgmt_job_info(self, mgmt_id):
        """
        Query the local HTCondor schedd for a management job ad.

        Args:
            mgmt_id (str or int): ClusterId of the management job to query.

        Returns:
            dict or None: A job ad dict with keys such as ``JobStatus``,
            ``EnteredCurrentStatus``, ``QDate``, and ``JobBatchName`` if the
            job is found; otherwise ``None``.
        """
        try:
            schedd = htcondor.Schedd()
            projection = ["JobStatus", "EnteredCurrentStatus", "QDate", "JobBatchName"]
            jobs = schedd.query(constraint=f"ClusterId == {mgmt_id}", projection=projection)
            return jobs[0] if jobs else None
        except Exception as e:
            # If schedd query fails, continue anyway - metadata has the important info
            print(f"Warning: Could not query schedd for management job info: {e}")
            return None
        
    def _display_workflow_status(self, mgmt_id, metadata, mgmt_job_ad=None):
        """
        Render and print a detailed workflow status summary.

        This function interprets cached metadata to compute counts of jobs in
        various states, produces a small progress bar and prints health
        indicators.

        Args:
            mgmt_id (str or int): Management ClusterId used to label output.
            metadata (dict): Parsed metadata containing job records and DAG info.
            mgmt_job_ad (dict or None): Optional management job ad from the
                schedd; when provided, additional runtime information is shown.

        Returns:
            None
        """
        # Management job information (optional - only if schedd query succeeded)
        if mgmt_job_ad:
            status_labels = {1: "idle", 2: "running", 3: "removed", 4: "completed", 5: "held"}
            htcondor_status = mgmt_job_ad.get("JobStatus", 0)
            mgmt_status = status_labels.get(htcondor_status, "unknown")
            batch_name = mgmt_job_ad.get("JobBatchName", f"snakemake-mgmt-{mgmt_id}")
            start_time = mgmt_job_ad.get("EnteredCurrentStatus") or mgmt_job_ad.get("QDate", time.time())
            duration = str(timedelta(seconds=int(time.time() - start_time)))
            print(f"Management Job {mgmt_id} [{batch_name}] has been {mgmt_status} for {duration}")
        else:
            print(f"Management Job {mgmt_id}")
        
        # Count node records from metadata
        ## already cached, no schedd queries needed
        jobs = metadata.get("jobs", {})
        cluster_ids = {
            job.get("cluster_id") for job in jobs.values() if job.get("cluster_id") is not None
        }
        executable_nodes = metadata.get("executable_nodes", 0)
        total_submitted = len(cluster_ids)
        total_nodes = len(jobs)

        # Get available status
        total_idle = sum(1 for j in jobs.values() if j.get("status", "").lower() == "idle")
        total_running = sum(1 for j in jobs.values() if j.get("status", "").lower() == "running")
        total_completed = sum(1 for j in jobs.values() if j.get("status", "").lower() == "completed")
        total_held = sum(1 for j in jobs.values() if j.get("status", "").lower() == "held")
        total_removed = sum(1 for j in jobs.values() if j.get("status", "").lower() == "removed") # not sure if we need this to display
        
        # Submitted job summary
        if total_submitted > 0:
            if total_nodes != total_submitted:
                print(
                    f"Workflow has submitted {total_submitted} HTCondor job(s), "
                    f"representing {total_nodes} node(s), of which:"
                )
            else:
                print(f"Workflow has submitted {total_submitted} job(s), of which:")
            if total_idle:
                print(f"        {total_idle} {'is' if total_idle == 1 else 'are'} submitted and waiting for resources.")
            if total_running:
                print(f"        {total_running} {'is' if total_running == 1 else 'are' } running.")
            if total_completed:
                print(f"        {total_completed} {'is' if total_completed == 1 else 'are'} completed.")
            if total_held:
                print(f"        {total_held} {'is' if total_held == 1 else 'are'} held.")
        else:
            print("No jobs have been submitted yet.")
        
        # Check if workflow is actually complete
        workflow_complete = (
            executable_nodes > 0 and 
            executable_nodes == total_completed and 
            total_removed == 0 and 
            total_held == 0 and 
            mgmt_job_ad is None # Management job has exited
        )

        # DAG summary
        waiting_on_dag = max(executable_nodes - total_nodes, 0)
        if executable_nodes > 0:
            # Instead of total nodes like DAGMan, we filter out nodes that are not submitted so we change the working here a little bit
            print(f"DAG contains {executable_nodes} total executable node(s), of which:") 
            if total_completed:
                print(f"    {colorize('[#]', Color.GREEN)} {colorize(str(total_completed), Color.GREEN)} {'has' if total_completed == 1 else 'have'} completed.")
            if total_running:
                print(f"    {colorize('[=]', Color.BLUE)} {colorize(str(total_running), Color.BLUE)} {'is' if total_running == 1 else 'are'} running.")
            # if total_idle:
            #     print(f"    [{Color.YELLOW}~{Color.END}] {total_idle} {'is' if total_idle == 1 else 'are'} submitted and waiting for resources.")
            if waiting_on_dag > 0:
                print(f"    {colorize('[-]', Color.YELLOW)} {colorize(str(waiting_on_dag), Color.YELLOW)} {'is' if waiting_on_dag == 1 else 'are'} waiting on other nodes to finish.")
            if total_held:
                print(f"    {colorize('[!]', Color.RED)} {colorize(str(total_held), Color.RED)} {'is' if total_held == 1 else 'are'} held.")
        
        # Health status summary
        if workflow_complete:
            print(f"{colorize("✓", Color.GREEN)} Workflow has completed successfully.")
        elif total_held > 0:
            print(f"{colorize("⚠", Color.RED)} Workflow has held jobs.")
        elif total_removed > 0:
            print(f"{colorize("✗", Color.RED)} Some jobs have been removed.")
        elif mgmt_job_ad and mgmt_job_ad.get("JobStatus") == 4:
            print(f"{colorize("✓", Color.GREEN)} Workflow has completed.")
        else:
            print(f"{colorize("→", Color.BLUE)} Workflow is running normally.")
        
        # Progress bar
        progress_total = executable_nodes or total_nodes
        if progress_total > 0:
            pct_completed = (total_completed / progress_total) * 100
            bar_width = 40
            completed_slots = int(total_completed / progress_total * bar_width)
            running_slots = int(total_running / progress_total * bar_width)
            waiting_slots = bar_width - completed_slots - running_slots
            
            bar = (f"{colorize('#' * completed_slots, Color.GREEN)}" +
                   f"{colorize('=' * running_slots, Color.BLUE)}" +
                   f"{colorize('-' * waiting_slots, Color.YELLOW)}")

            if workflow_complete:
                print(f"[{colorize('#' * bar_width, Color.GREEN)}] Workflow is 100% complete.")
            else:
                print(f"[{bar}] Workflow is {pct_completed:.1f}% complete.")
        
class Snake(Noun):
    """
    Run operations on Snakemake workflows via HTCondor
    """

    class submit(Submit):
        pass
    
    class status(Status):
        pass

    @classmethod
    def verbs(cls):
        return [cls.submit, cls.status] 
