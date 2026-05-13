import htcondor2 as htcondor
import shutil
import subprocess
import sys
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from htcondor_cli.noun import Noun
from htcondor_cli.verb import Verb
import traceback
import re

class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    BLUE = '\033[94m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'

"""
HTCondor CLI for running Snakemake workflow
"""
from htcondor_cli.noun import Noun
from htcondor_cli.verb import Verb

class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    BLUE = '\033[94m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'

class Submit(Verb):
    """
    Submit a local universe job and Snakemake jobs when run.
    """
    # command-line argument configurations
    options = {
        "snakefile": {
            "args": ("snakefile",), # positional argument
            "help": "Positional argument to Snakefile. If omitted, Snakefile is assumed to be at the current directory.",
            "nargs": "?",
        },
        "profile": {
            "args": ("--profile",),
            "help": "Optional flag to specify the path of Snakemake profile directory. You can also specify this flag after the seperator `--`",
            "required": False,
        }, 
        "jobdir": {
            "args": ("--jobdir",),
            "help": "Optional directory for HTCondor log files. If omitted, a `logs` directory will be created at the current directory. If `--htcondor-jobdir` is not specified for Snakemake's logs, all log files will be placed under `--jobdir` location.",
            "required": False,
        },
        "executor": {
            "args": ("--executor",),
            "help": "Required Snakemake-HTCondor executor plugin to be sent to the execution point (EP). Please install the executor beforehand.",
            "required": True,
        }
    }

    def __init__(self, logger, snakefile, snakemake_args=None, **options):
        # Basic validations of CLI
        snakefile = self._validate_snakefile(snakefile)
        profile = self._validate_profile(options) # can be None depends on users' choices
        jobdir = self._setup_jobdir(options)
        executor = options.get("executor")

        # submit a local universe job
        try:
            self._submit_local(snakefile, profile, jobdir, executor, snakemake_args or [])
        except Exception as e:
            print("Error: Could not submit local universe job.")
            print(f"Exception details:", str(e))
            traceback.print_exc()
            raise

    def _validate_snakefile(self, snakefile):
        """Minimal validation: file exists"""
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

    def _validate_profile(self, options):
        """Return profile path if it is explicitly provided by user"""
        profile_specified = options.get("profile")
        if profile_specified:
            profile = Path(profile_specified) 
            if not profile.exists():
                raise FileNotFoundError(f"Profile directory not found: {profile}")
            return profile
        return None

    def _setup_jobdir(self, options):
        """Create log directory if needed"""
        if options.get("jobdir"):
            jobdir = Path(options.get("jobdir"))
        else:
            jobdir = Path.cwd() / "logs"
        
        jobdir.mkdir(parents=True, exist_ok=True)
        return jobdir

    # ===== HTCondor SUBMISSION METHODS ===== #
    def _submit_local(self, snakefile, profile, jobdir, executor, snakemake_args):
        """Submit snakemake as an HTCondor local universe job."""
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
            f"--executor {executor}",
            f"--htcondor-jobdir {jobdir}"
        ]

        # Add profile if specified
        if profile:
            args_list.append(f"--profile {profile}")
        
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

        print(f"Snakemake managment job submitted with JobID {cluster_id}.0")
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
        Display workflow status from cached metadata (avoids expensive schedd queries).
        Metadata file location is always .snakemake/htcondor/snakemake-metadata-clusterid.json
        and contains the jobdir that user may have customized.
        """
        self.logger = logger

        if mgmt_id is None:
            print("Error: management ID is required")
            return
        
        try:
            # Metadata is always at this fixed location, try relative path
            metadata_path = Path(".snakemake/htcondor") / f"snakemake-metadata-{mgmt_id}.json"

            if not metadata_path.exists():
                print(f"Error: Metadata not found at {metadata_path}")
                print("Make sure you run from the directory where 'htcondor snake submit' was executed.")
                return
            
            with open(metadata_path) as f:
                metadata = json.load(f)
            
            # Now if metadata has the absolute path, we could use it for future lookups
            metadata_dir_abs = metadata.get("metadata_dir")
            if metadata_dir_abs:
                self.logger.debug(f"Using metadata dir: {metadata_dir_abs}")
            
            # jobdir = metadata.get("jobdir")
            # if not jobdir:
            #     print("Error: jobdir not found in metadata file")
            #     return
            
            self._show_status(mgmt_id, metadata)
            
        except Exception as e:
            print(f"Error: Could not get status for job {mgmt_id}")
            print(f"Exception: {str(e)}")
            traceback.print_exc()
    
    def _show_status(self, mgmt_id, metadata):
        """Read metadata file and display workflow status"""
        # Query schedd only for management job info (elapsed time, status)
        mgmt_job_ad = self._get_mgmt_job_info(mgmt_id)
        
        # Display using metadata for job statuses (efficient, no schedd queries)
        self._display_workflow_status(mgmt_id, metadata, mgmt_job_ad)
    
    def _get_mgmt_job_info(self, mgmt_id):
        """Query schedd only for management job info - single efficient query"""
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
        """Display workflow status using metadata (efficient, no schedd queries)"""
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
        
        # Count jobs from metadata (already cached, no schedd queries needed)
        jobs = metadata.get("jobs", {})
        dag_nodes = metadata.get("dag_nodes", 0)
        
        total_idle = sum(1 for j in jobs.values() if j["status"] == "idle")
        total_running = sum(1 for j in jobs.values() if j["status"] == "running")
        total_completed = sum(1 for j in jobs.values() if j["status"] == "completed")
        total_held = sum(1 for j in jobs.values() if j["status"] == "held")
        total_removed = sum(1 for j in jobs.values() if j["status"] == "removed")
        total_submitted = len(jobs)
        
        # Submitted job summary
        if total_submitted > 0:
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
            total_submitted > 0 and 
            total_submitted == total_completed and 
            total_removed == 0 and 
            total_held == 0 and 
            mgmt_job_ad is None # Management job has exited
        )
        # DAG summary
        waiting_on_dag = max(dag_nodes - total_submitted, 0)
        if dag_nodes > 0:
            print(f"DAG contains {dag_nodes} node(s) total, of which:")
            if total_completed:
                print(f"    [{Colors.GREEN}#{Colors.END}] {total_completed} {'has' if total_completed == 1 else 'have'} completed.")
            if total_running:
                print(f"    [{Colors.BLUE}={Colors.END}] {total_running} {'is' if total_running == 1 else 'are'} running.")
            # if total_idle:
            #     print(f"    [{Colors.YELLOW}~{Colors.END}] {total_idle} {'is' if total_idle == 1 else 'are'} submitted and waiting for resources.")
            if waiting_on_dag > 0:
                print(f"    [{Colors.YELLOW}-{Colors.END}] {waiting_on_dag} {'is' if waiting_on_dag == 1 else 'are'} waiting on other nodes to finish.")
            if total_held:
                print(f"    [{Colors.RED}!{Colors.END}] {total_held} {'is' if total_held == 1 else 'are'} held.")
        
        # Health status summary
        if workflow_complete:
            print(f"{Colors.GREEN}✓{Colors.END} Workflow has completed successfully.")
        elif total_held > 0:
            print(f"{Colors.RED}⚠{Colors.END} Workflow has held jobs.")
        elif total_removed > 0:
            print(f"{Colors.RED}✗{Colors.END} Some jobs have been removed.")
        elif mgmt_job_ad and mgmt_job_ad.get("JobStatus") == 4:
            print(f"{Colors.GREEN}✓{Colors.END} Workflow has completed.")
        else:
            print(f"{Colors.BLUE}→{Colors.END} Workflow is running normally.")
        
        # Progress bar
        if total_submitted > 0:
            pct_completed = (total_completed / total_submitted) * 100
            bar_width = 40
            completed_slots = int(total_completed / total_submitted * bar_width)
            running_slots = int(total_running / total_submitted * bar_width)
            waiting_slots = bar_width - completed_slots - running_slots
            
            bar = (f"{Colors.GREEN}{'#' * completed_slots}{Colors.END}" +
                   f"{Colors.BLUE}{'=' * running_slots}{Colors.END}" +
                   f"{Colors.YELLOW}{'-' * waiting_slots}{Colors.END}")

            if workflow_complete:
                print(f"[{Colors.GREEN}{'#' * bar_width}{Colors.END}] Workflow is 100% complete.")
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





