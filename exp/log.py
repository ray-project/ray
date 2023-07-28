import os
import logging

# Get the current process ID
pid = os.getpid()

# Create a logger
logger = logging.getLogger(__name__)

# Set the log level to INFO
logger.setLevel(logging.INFO)

# Create a file handler (from the current working dir)
handler = logging.FileHandler('logs/process_{}.log'.format(pid))

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set the formatter for the handler
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

# Log some messages
local_rank = os.environ.get('LOCAL_RANK', 'N/A')
rank = os.environ.get('RANK', 'N/A')
group_rank = os.environ.get('GROUP_RANK', 'N/A')
role_rank = os.environ.get('ROLE_RANK', 'N/A')
local_world_size= os.environ.get('LOCAL_WORLD_SIZE', 'N/A')
world_size = os.environ.get('WORLD_SIZE', 'N/A')
role_world_size = os.environ.get('ROLE_WORLD_SIZE', 'N/A')
master_addr = os.environ.get('MASTER_ADDR', 'N/A')
master_port = os.environ.get('MASTER_PORT', 'N/A')
torchelastic_restart_count = os.environ.get('TORCHELASTIC_RESTART_COUNT', 'N/A')
torchelastic_max_restarts = os.environ.get('TORCHELASTIC_MAX_RESTARTS', 'N/A')
torchelastic_run_id = os.environ.get('TORCHELASTIC_RUN_ID', 'N/A')
python_exec = os.environ.get('PYTHON_EXE', 'N/A')

# Log those info to stdout
logger.info('local_rank: {}'.format(local_rank))
logger.info('rank: {}'.format(rank))
logger.info('group_rank: {}'.format(group_rank))
logger.info('role_rank: {}'.format(role_rank))
logger.info('local_world_size: {}'.format(local_world_size))
logger.info('world_size: {}'.format(world_size))
logger.info('role_world_size: {}'.format(role_world_size))
logger.info('master_addr: {}'.format(master_addr))
logger.info('master_port: {}'.format(master_port))
logger.info('torchelastic_restart_count: {}'.format(torchelastic_restart_count))
logger.info('torchelastic_max_restarts: {}'.format(torchelastic_max_restarts))
logger.info('torchelastic_run_id: {}'.format(torchelastic_run_id))
logger.info('python_exec: {}'.format(python_exec))
