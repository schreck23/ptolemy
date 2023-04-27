Ptolemy (Beta release v0):
    
A data processing engine that is designed to help with data onboarding within 
the Filecoin network.

Ptolemy will consist of a series of component designed to help process entire 
existing file structures and allow users to efficiently package data for 
delivery to Filecoin Storage Providers (from here on in referred to as SPs).  
This allows to track and associate legacy filenames with CIDs, build car files 
of a configurable size and even invoke deals with deal engines on the network.

To get started with Ptolemy in its current iteration you can use the http 
interface to configure and run jobs in a mode known as blitz which will attempt
to build all car files in parallel.  A future version of ptolemy (coming soon)
will have a serial mode to support processing environments where there is 
limited staging capacity.

To better understand how Ptolemy works there are a handful components involved 
in its successful operation.  In order to support Ptolemy a couple of existing
ecosystem products are used in tandem with Ptolemy.  There are the following:

https://github.com/ipld/go-car - Used to package up the prepared sectors
(in this case directories) and create the car file.  Once this is installed 
the path to the go-car binary should be configured in worker.ini.

https://github.com/filecoin-project/go-fil-commp-hashhash - Used to calculate
the commp for the newly generated car file.  This must be installed prior to 
use and the path to the stream-commp binary must be configured in the 
worker.ini file.
         
    INSTALL (for Ubuntu 22.04)
    
    # Setup base environment
    sudo apt-get update
    sudo apt-get install git
    sudo apt-get install python3
    sudo apt-get install python3-pip
    
    # add python specific libraries
    sudo pip3 install fastapi
    sudo apt-get -y install python3-psycopg2
    sudo pip3 install uvicorn
        
    cd /usr/local
    sudo git clone https://github.com/schreck23/ptolemy
    
    # database prep
    sudo apt -y install postgresql postgresql-contrib
    sudo systemctl start postgresql.service
    sudo systemctl enable postgresql.service
    
    # configure database for Ptolemy user
    # (note) - you can create a db with any name and user account with any
    # credentials you wish, however the database.ini will need to be updated
    # based on those parameters
    sudo -u postgres createuser -d -s repository
    sudo -u postgres psql -c "ALTER USER repository PASSWORD 'ptolemy'";
    sudo -u postgres createdb ptolemy
    
    CONFIGURE 
    
    To get started open the ptolemy.ini file found in /usr/local/ptolemy and 
    set each flag properly as defined below:
    
    ip_addr -> The IP address we wish the orchestrator to listen on, if you
    wish to just universally handle connections then 0.0.0.0 can be used.  If
    you wish to use the external machine IP that will work as well.
    
    port -> the port you wish to have the Ptolemy orchestrator listen on.
    
    api_threads -> the number of threads you wish to allocate to listen for 
    any and all api requests
    
    threads -> number of threads allocated to the scan process for the 
    ptolemy orchestrator
    
    Upon completing the configuration for the orchestrator the worker must now
    be configured.  There are two sections in this config 
    file(/usr/local/ptolemy/worker.ini), the first being 
    the orchestrator section and the second being the worker section.
    
    orchestrator section:
    ip_addr -> the ip address of the Ptolemy orchestrator node.
    port -> the listener port for the Ptolemy orchestrator
    
    worker section:
    ip_addr -> the ip address for the host node, due to the heartbeat mechanism
    the 0.0.0.0 address cannot be used here.  Only 1 worker per IP is allowed.
    
    port -> port we wish to have the worker listen on.
    
    threads -> number of threads we wish to use for creating our car files in
    parallel
    
    api_threads -> number of threads you wish to allocate to listen for http
    requests
    
    car_gen -> path to go-car binary to use for making carfiles
    
    commp -> path to the commp binary to use for calculating commp on car
    files
    
    We must also setup our database credentials in the
    /usr/local/ptolemy/database.ini file.
    
    host -> the host where postgres is currently installed
    
    db_name -> the name of the db where we want to place the tables for 
    ptolemy (the default is ptolemy but if a different one was used during
    install place it here)
    
    db_user -> the user for all ptolemy interactions, if you used a different
    user name above (repository in this case) please use it here
    
    pass -> the password for the user, by default this would be ptolemy but if
    a different password was configured above use it here.
    
    The last config file in /usr/local/ptolemy is the logging.ini file.  This
    will configure the python logger for ptolemy and the worker which will 
    share a common logfile.
    
    log_level -> The desired logging level we wish to use, options include 
    INFO, DEBUG, WARN, ERROR & CRITICAL
    
    format -> used to configure the format of the log message
    
    datefmt -> can be used to customize the format of the time stamp 
    associated with the log message
    
    logfile -> path and filename for logging messages, the default is 
    /tmp/ptolemy.log
    
    It should be noted that all entries in this configuration file should
    follow traditional python logging standards.  This also includes the need
    for redundant pound signs to provide a necessary escape character.  This
    will allow users to configure the logging service to produce logs that 
    may be shared with other applications like Splunk for monitoring.
    
    USE PTOLEMY
    
    Now that we have Ptolemy configured it is time to create a project and
    generate the respective car files.  For this example will use a host of 
    192.168.1.100 which will be our orchestrator IP.  It should be noted that
    if the worker is properly configured to communicate with the orchestrator
    it will auto-register to receive instructions.
    
    The Ptolemy startup will be placed in a system startup script in the future
    but for now the operating instructions are quite simple.
    
    cd /usr/local/ptolemy
    sudo python3 ptolemy.py
    
    If we are running the worker on the same node then also do the following:
    cd /usr/local/ptolemy
    sudo python3 worker.py
    
    Now that Ptolemy is running we will configure and project and launch it, 
    to do this we will use the API to configure a project, scan the file
    system, align the files in containers for car files and then execute the 
    build.  It should be noted that the commands issued to the API will
    return a positive or negative response immediately to indicate whether the
    step failed or a background process has been launched.
    
    At any given point a project's status can be obtained by using the 
    following RESTful call:
    
    curl -X GET http://<Ptolemy ip>:<Ptolemy port>/v0/projects/
    
    This will return a list of all the projects maintained by Ptolemy and 
    their respective status.
    
    To understand how to configure a project we need to understand the meta
    associated with the configuration:
    
    shard_size - this will indicate a threshold for the largest file size in 
    GiBs and will support an integer between 1 and 31.  This setting will break
    a file into chunks of this size if it is larger than the shard_size.  For 
    example if the shard_size is 1 a 10 GiB file will result in 10 chunks.

    staging_dir - a directory set aside for local cache where files can be 
    chopped and car files can be built and placed for extraction.

    target_dir - directory where our dataset lives (in this case an 
    unstructured dataset in a filesystem typically) that we wish to convert to 
    car files.

    car_size - an integer between 1 and 31 that will allow a user to determine 
    how large the generated cars should be in GiBs.

    encryption - currently not implemented but in a future ptolemy release 
    users will be able to specify a key alias representing a specific key the 
    user wishes to encrypt data with if data privacy is desired.

    load_type - blitz vs serial, currently only blitz is supported but serial 
    mode will be released soon.
    
    Now that we understand our metadata requirement lets create a ficticious
    project named delta with some set of metadata.
    
    shard_size = 1 GiB
    staging_dir = /srv/delta-staging/shrek-staging/ptolemy-test
    target_dir = /srv/delta-staging/shrek-staging/radiant-data-to-delta
    car_size = 17 GiBs
    encryption = None
    load_type = blitz

    To create and prepare this project issue the following command
    (without carriage returns of course):
    
    curl -X POST http://<ptolemy ip address>:<ptolemy port>/v0/create/delta 
    -H 'Content-Type: application/json' 
    -d '{"shard_size":1,
    "staging_dir":"/srv/delta-staging/shrek-staging/ptolemy-test",
    "target_dir":"/srv/delta-staging/shrek-staging/radiant-data-to-delta",
    "car_size":17,"encryption":"None","load_type":"blitz"}'
    
    Now that we have defined our project we are now ready to scan the 
    filesystem and capture the relevant metadata:
    
    curl -X POST http://<ptolemy ip address>:<ptolemy port>/v0/scan/delta
    
    When this background task is done the job state will be updated to 
    "scanning complete", using the status command above we can check to see
    when it's done or we can look directory in the postgres DB if we choose,
    data is preserved in the ptolemy_projects table.
    
    Once scanning is done we containerize and create our sector mapping by
    using the following command:
    
    curl -X POST http://<ptolemy ip address>:<ptolemy port>/v0/containerize/delta
    
    When the background task is complete the job status will show 
    "containerization complete" we can then begin making car files with our
    new containers.
    
    In the beta version only blitz mode is active so once the containerization
    is complete build your carfiles:
    
    curl -X POST http://<ptolemy ip address>:<ptolemy port>/v0/blitz/delta
    
    This will place your carfiles in your staging area, calculate the root
    CID and commp and place that metadata in the database.  To obtain the
    carfile metadata use the following command:
    
    curl -X GET http://<ptolemy ip address>:<ptolemy port>/v0/carfile_meta/delta
    
    ROADMAP
    
    Ptolemy beta is currently in version v0 and all API calls are categorized 
    using this nomenclature.  Any new major version will use a new set of 
    URLs with a v<major version> tag as new features are added.  Deprecation
    periods will be provided as new versions replace old versions so as not 
    to cause significant API impacts.
    
    Ptolemy V1 plans:
    
    - Direct from S3 egress and car build.  Already in development and will 
    designed to support S3 open data with no credentials and tradtional AWS
    v2/4 credentials as well.
    
    - Preliminary UI release is scheduled for V1 (buiding off delta and
    delta-dm UI frameworks)
    
    - File system listener that will capture changes to a scanned filesystem 
    and track them for reconciliation.
    
    - Storage protocol support for NAS like NFS/CIFS mounting
    
    - Updates for logging and better state management and error pathing 
    (still a WIP)
    
    - Encryption support
    
    Ptolemy V2 plans:
    
    - Other cloud targets (potentially Azure and GCP and others)
    
    - Car v2 support
    
    - Direct to delta integration for end to end automated pipeline flow
