from logging import Logger
from pathlib import Path
from subprocess import PIPE, Popen
from time import sleep
from util.logging_util import log_message


def launch_process(commands_string: str) -> tuple:
    process = Popen(args=commands_string,
                    stdout=PIPE,
                    universal_newlines=True,
                    shell=True)
    process_stdout = []
    for line in iter(lambda: process.stdout.readline(), ""):
        line = "\n".join([line for line in line.splitlines() if line])
        process_stdout.append(line)
    return_code = process.poll()
    return return_code, process_stdout


def execute_command(command: str,
                    on_new_windows: bool,
                    max_tries: int,
                    time_between_retries_in_seconds: int,
                    logger: Logger,
                    logger_level: str) -> list:
    commands_string = ""
    if on_new_windows:
        commands_string = "gnome-terminal -- "
    commands_string = commands_string + "{0}".format(command)
    message = "Executing the '{0}' command locally...".format(command)
    log_message(logger, message, logger_level)
    current_try = 1
    return_code = None
    process_stdout = None
    while current_try < max_tries:
        return_code, process_stdout = launch_process(commands_string=commands_string)
        if return_code == 0:
            message = "The command '{0}' was successfully executed locally!".format(command)
            log_message(logger, message, logger_level)
            break
        else:
            message = "Error while trying to execute the command '{0}' locally! Retrying...".format(command)
            log_message(logger, message, logger_level)
            current_try = current_try + 1
            sleep(time_between_retries_in_seconds)
    if return_code != 0:
        message = "Fatal Error! Exiting..."
        log_message(logger, message, logger_level)
        exit(1)
    return process_stdout


def remotely_execute_command(key_file: Path,
                             username: str,
                             public_ipv4_address: str,
                             ssh_port: str,
                             remote_command: str,
                             on_new_windows: bool,
                             request_tty: bool,
                             max_tries: int,
                             time_between_retries_in_seconds: int,
                             logger: Logger,
                             logger_level: str) -> list:
    commands_string = ""
    if on_new_windows:
        commands_string = "gnome-terminal -- "
    tty = ""
    if request_tty:
        tty = "-t"
    commands_string = commands_string + "ssh {0} -i {1} {2}@{3} -p {4} \"{5}\"" \
        .format(tty,
                key_file,
                username,
                public_ipv4_address,
                ssh_port,
                remote_command)
    message = "Executing the '{0}' command on the remote host '{1}'..." \
        .format(remote_command,
                public_ipv4_address)
    log_message(logger, message, logger_level)
    current_try = 1
    return_code = None
    process_stdout = None
    while current_try < max_tries:
        return_code, process_stdout = launch_process(commands_string=commands_string)
        if return_code == 0:
            message = "The command '{0}' was successfully executed on the remote host '{1}'!" \
                .format(remote_command,
                        public_ipv4_address)
            log_message(logger, message, logger_level)
            break
        else:
            message = "Error while trying to execute the command '{0}' on the remote host '{1}'! Retrying..." \
                .format(remote_command,
                        public_ipv4_address)
            log_message(logger, message, logger_level)
            current_try = current_try + 1
            sleep(time_between_retries_in_seconds)
    if return_code != 0:
        message = "Fatal Error! Exiting..."
        log_message(logger, message, logger_level)
        exit(1)
    return process_stdout
