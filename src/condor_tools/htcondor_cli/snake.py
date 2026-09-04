import argparse
import htcondor2 as htcondor
import shutil
import importlib.util
import sys
import os
import signal
import time
from datetime import timedelta
from pathlib import Path
import traceback
import argparse

"""
HTCondor CLI for running Snakemake workflow
"""
from htcondor_cli.noun import Noun
from htcondor_cli.verb import Verb

class Submit(Verb):
    """
    Submit a local universe job and Snakemake jobs when run.
    """
    # Command-line argument configurations
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
        },
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
        # Check for the presence of the executor plugin
        if importlib.util.find_spec("snakemake_executor_plugin_htcondor") is None:
            raise RuntimeError(
                "The 'snakemake-executor-plugin-htcondor' plugin is required but not yet installed.\n"
                "Install it with: pip install snakemake-executor-plugin-htcondor"
            )
            

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


        jobdir = self._setup_jobdir(options)

        # Basic validations for Snakefile
        snakefile = self._validate_snakefile(snakefile)
                    
        # Submit a local universe job
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
        try:
            separator_index = snakemake_args.index('--')
        except ValueError:
            separator_index = None
        
        # Scan and extract all --jobdir occurrences from before the separator
        scan_range = separator_index if separator_index is not None else len(snakemake_args)
        
        # Collect all --jobdir values (last one will be used)
        filtered_args = []
        jobdir_value_in_remainder = None
        i = 0

        # For repeated specifications of --jobdir, the last one wins
        while i < scan_range:
            if snakemake_args[i] == '--jobdir':
                if i + 1 >= scan_range or snakemake_args[i+1].startswith("-"):
                    raise ValueError("--jobdir requires a valid directory string value")
                jobdir_value_in_remainder = snakemake_args[i+1]
                i += 2
            else:
                filtered_args.append(snakemake_args[i])
                i += 1
        
        if jobdir_value_in_remainder is None:
            return snakemake_args

        options["jobdir"] = jobdir_value_in_remainder
        filtered_args.extend(snakemake_args[scan_range:])
        return filtered_args
    
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

        # If snakefile is not provided, use default
        if snakefile is None:
            snakefile = "Snakefile"
        
        snakefile_path = Path(snakefile)
        if not snakefile_path.exists():
            raise FileNotFoundError(
                f"Could not find Snakefile: {snakefile}\n"
                f"Make sure to provide the correct path to the Snakefile or place 'Snakefile' in the current directory."
            )
        return snakefile_path

    def _setup_jobdir(self, options):
        """
        Create or resolve the job directory for management logs.

        Args:
            options (dict): Parsed CLI options. If the dict contains a
                "jobdir" key its value will be used as the job directory.

        Returns:
            pathlib.Path: Absolute path to the created or existing job
            directory. Resolved to absolute here since
            it's later embedded in the management job's ClassAd
            (`HTCondorSnakeMetadata`) and must resolve correctly regardless
            of the working directory `status` is later run from.
        """

        if options.get("jobdir"):
            jobdir = Path(options.get("jobdir"))
        else:
            jobdir = Path.cwd() / "logs" # default name if jobdir is not provided

        jobdir = jobdir.resolve()
        jobdir.mkdir(parents=True, exist_ok=True)
        return jobdir

    # ===== HTCondor SUBMISSION METHODS ===== #
    def _submit_local(self, snakefile, jobdir, snakemake_args):
        """
        Submit Snakemake as an HTCondor local-universe management job.

        This method discovers the Snakemake executable, constructs a
        Submit description for HTCondor, submits the job, and prints
        submission info. The metadata file path is set as a custom
        ClassAd attribute (`HTCondorSnakeMetadata`) on the management
        job so other commands (eg. `status`) can locate it via the schedd.

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
            f"--htcondor-jobdir {jobdir}",
        ]

        # Append any additional snakemake args passed after the -- separator
        if snakemake_args:
            args_list.extend(snakemake_args)

        arguments = " ".join(args_list)

        request_memory = htcondor.param.get("SNAKEMAKE_MANAGER_REQUEST_MEM", "512MB")

        submit_description = htcondor.Submit({
            "executable": snakemake_path,
            "arguments": arguments,
            "universe": "local",
            "request_disk": "512MB",
            "request_cpus": 1,
            "request_memory": request_memory,
            
            # Set up logging
            "log": f"{jobdir}/snakemake-mgmt-$(ClusterId).log",
            "output": f"{jobdir}/snakemake-mgmt-$(ClusterId).out",
            "error": f"{jobdir}/snakemake-mgmt-$(ClusterId).err",
            
            # Specify getenv so the job uses the submitter's environment
            "getenv": "true",
            
            # Management Job Name
            "JobBatchName": f"snakemake-mgmt-$(ClusterId)",

            # Pointer to the metadata file, set on the ClassAd so `htcondor snake
            # status <mgmt_id>` can read it straight from the schedd. 
            "MY.HTCondorSnakeMetadata": f'"{jobdir}/snakemake-metadata-$(ClusterId).json"',
        })
        
        # Submit to HTCondor
        schedd = htcondor.Schedd()
        submit_result = schedd.submit(submit_description)
        
        cluster_id = submit_result.cluster()
        print(f"Snakemake managment job submitted with JobID {cluster_id}.0")
        print(f"Logs can be found in {jobdir}")

class Halt(Verb):
    """
    Halt the progress of a workflow given the management job ID
    """
    options = {
        "mgmt_id": {
            "args": ("mgmt_id",),
            "type": int,
            "help": "Positional argument for a management JobID that oversees the entire workflow. Must be specified."
        },
    }

    def __init__(self, logger, mgmt_id=None, **option):
        """
        When `htcondor snake halt <mgmt_id>` is run, pause the workflow by allowing 
        the currently running jobs finish and stop any jobs that are not yet submitted.

        Args:
            logger: Logger object used for logging messages.
            mgmt_id (str or int): Management job ClusterId for the workflow.
            **options: Reserved for future options.

        Returns:
            None

        Raises:
            RuntimeError: if the schedd cannot be reached.
            
        """

        self.logger = logger

        if mgmt_id is None:
            print("Error: management job ID is required.")
            sys.exit(1)
        
        try:
            mgmt_id = int(mgmt_id)
        except ValueError:
            print("Management job ID must be an integer.")
            sys.exit(1)
        
        # Querying the schedd for the saved PID: SnakeMgmtPID (what happen when are there two managment job?)

        # To minimized TOCTOU, we will, at the same time, ask for the JobStatus together with SnakeMgmtPID
        # and only send SIGTERM to this PID when the job is still running.
        # TOCTOU still exists but less common in modern linux OS unless users are running
        # in the containers or APs that are very busy.

        try:
            schedd = htcondor.Schedd()

            # 1. Query SnakeMgmtPID
            jobs = schedd.query(constraint=f"ClusterId == {mgmt_id}", projection=["JobStatus", "SnakeMgmtPID"])

            if not jobs:
                self.logger.error(f"No management job found with JobID {mgmt_id}.")
                sys.exit(1)

            target_pid = None
            for job in jobs:
                status = job.get("JobStatus")
                # making sure it is actively running
                if status == 2:
                    target_pid = job.get("SnakeMgmtPID")
                    break

            if target_pid is None:
                self.logger.warning(f"Management job {mgmt_id} is not currently running; nothing to halt.")
                sys.exit(1)

            # 2. Sending SIGTERM to management job.
            # Snakemake catches SIGTERM and shuts down gracefully: it lets
            # already-running jobs finish and stops submitting new ones.
            try:
                os.kill(target_pid, signal.SIGTERM)
            except ProcessLookupError:
                self.logger.warning(f"Management job {mgmt_id} process (PID {target_pid}) is no longer running.")
                sys.exit(1)

            print(f"Halting the management job {mgmt_id}.")

        except Exception as e:
            self.logger.error(f"Could not halt the management job: {e}")
            sys.exit(1)


class Snake(Noun):
    """
    Run operations on Snakemake workflows via HTCondor
    """

    class submit(Submit):
        pass

    class halt(Halt):
        pass

    @classmethod
    def verbs(cls):
        return [cls.submit, cls.halt] 
