# Working with The High Performance Storage System (HPSS) at NERSC

HPSS is the tape-based data storage system we use for long term storage of experimental data at the ALS. Tape storage, while it may seem antiquated, is still a very economical and secure medium for infrequently accessed data as tape does not need to be powered except for reading and writing. This requires certain considerations when working with this system.

In `orchestration/transfer_controller.py` we have included two transfer classes for moving data from CFS to HPSS and vice versa (HPSS to CFS). We are following the [HPSS best practices](https://docs.nersc.gov/filesystems/HPSS-best-practices/) outlined in the NERSC documentation.

HPSS is intended for long-term storage of data that is not frequently accessed, and users should aim for file sizes between 100 GB and 2 TB. Since HPSS is a tape system, we need to ensure storage and retrieval commands are done efficiently, as it is a mechanical process to load in a tape and then scroll to the correct region on the tape.

While there are Globus endpoints for HPSS, the NERSC documentation recommends against it as there are certain conditions (i.e. network disconnection) that are not as robust as their recommended HPSS tools `hsi` and `htar`, which they say is the fastest approach. Together, these tools allow us to work with the HPSS filesystem and carefully bundle our projects into `tar` archives that are built directly on HPSS.

## Working with `hsi`

We use `hsi` for handling individual files on HPSS. [Here is the official NERSC documentation for `hsi`.](https://docs.nersc.gov/filesystems/hsi/)


**Login to HPSS using `hsi`**

```
nersc$ hsi
```


**Common `hsi` commands**
```
hsi ls: show the contents of your HPSS home directory
hsi mkdir [new_dir]: create a remote directory in your home
hsi put [local_file_name]: Transfer a single file into HPSS with the same name
hsi put -R [local_directory]: Transfer a directory tree into HPSS, creating sub-dirs when needed
hsi get [/path/to/hpss_file]: Transfer a single file from HPSS into the local directory without renaming
hsi rm [/path/to/hpss_file]: Prune a file from HPSS
hsi rm -r [/path/to/hpss_file]: Prune a directory from HPSS
hsi rmdir /path/to/my_hpss_dir/: Prune an empty directory

```

**Examples**

Find files that are more than 20 days old and redirects the output to the file temp.txt:

```
hsi -q "find . -ctime 20" > temp.txt 2>&1
```

## Working with `htar`

We can use `htar` to efficiently work with groups of files on HPSS. The basic syntax of `htar` is similar to the standard `tar` utility:

```
htar -{c|K|t|x|X} -f tarfile [directories] [files]

-c : Create
-K : Verify existing tarfile in HPSS
-t : List
-x : Extract
-X : re-create the index file for an existing archive
```

You cannot add or append files to an existing htar file. The following examples [can also be found here](https://docs.nersc.gov/filesystems/htar/#htar-usage-examples).

**Create an archive with a directory and file**

```
nersc$ htar -cvf archive.tar project_directory some_extra_file.json
```
**List the contents of a `tar` archive**
```
nersc$ htar -tf archive.tar
HTAR: drwx------  als/als          0 2010-09-24 14:24  project_directory/cool_scan1
HTAR: -rwx------  als/als    9331200 2010-09-24 14:24  project_directory/cool_scan2
HTAR: -rwx------  als/als    9331200 2010-09-24 14:24  project_directory/cool_scan3
HTAR: -rwx------  als/als    9331200 2010-09-24 14:24  project_directory/cool_scan4
HTAR: -rwx------  als/als     398552 2010-09-24 17:35  some_extra_file.json
HTAR: HTAR SUCCESSFUL

```

**Extract the entire `htar` file**

```
htar -xvf archive.tar
```

**Extract a single file from `htar`**

```
htar -xvf archive.tar project_directory/cool_scan4
```

**`-Hnostage` option**

If your `htar` files are >100GB, and you only want to extract one or two small member files, you may find faster retrieval rates by skipping staging the file to the HPSS disk cache with `-Hnostage`.

```
htar -Hnostage -xvf archive.tar project_directory/cool_scan4
```

## Transferring Data from CFS to HPSS

NERSC provides a special `xfer` QOS ("Quality of Service") for interacting with HPSS, which we can use with our SFAPI Slurm job scripts.

### Single Files

We can transfer single files over to HPSS using `hsi put` in a Slurm script:

**Example `hsi` transfer job**

```
#SBATCH --qos=xfer
#SBATCH -C cron
#SBATCH --time=12:00:00
#SBATCH --job-name=my_transfer
#SBATCH --licenses=SCRATCH
#SBATCH --mem=20GB

# Archive a user's project folder to HPSS
hsi put /global/cfs/cdirs/als/data_mover/8.3.2/raw/als_user_project_folder/cool_scan1.h5
```

Notes:
- `xfer` jobs specifying -N nodes will be rejected at submission time. By default, `xfer` jobs get 2GB of memory allocated. The memory footprint scales somewhat with the size of the file, so if you're archiving larger files, you'll need to request more memory. You can do this by adding `#SBATCH --mem=XGB` to the above script (where X in the range of 5 - 10 GB is a good starting point for large files).
- NERSC users are at most allowed 15 concurrent `xfer` sessions, which can be used strategically for parallel transfers and reads.


### Multiple Files

NERSC recommends that when serving many files smaller than 100 GB we use `htar` to bundle them together before archiving. Since individual scans within a project may not be this large, we try to archive all of the scans in a project into a single `tar` file. If projects end up being larger than 2 TB, we can create multiple `tar` files.

One great part about `htar` is that it builds the archive directly on `HPSS`, so you do not need to worry about needing the additional storage allocation on the CFS side for the `tar` file.

**Example `xfer` transfer job**
```
#SBATCH --qos=xfer
#SBATCH -C cron
#SBATCH --time=12:00:00
#SBATCH --job-name=my_transfer
#SBATCH --licenses=SCRATCH
#SBATCH --mem=100GB

# Archive a user's project folder to HPSS
htar -cvf als_user_project.tar /global/cfs/cdirs/als/data_mover/8.3.2/raw/als_user_project_folder
```

## Transferring Data from HPSS to CFS

At some point you may want to access data from HPSS. An important thing to consider is whether you need to access single or multiple files.

You could extract an entire `htar` file

```
htar -xvf als_user_project_folder.tar
```

Or maybe a single file 

```
htar -xvf als_user_project_folder.tar cool_scan1.h5
```

## Prefect Flows for HPSS Transfers

Most of the time we expect transfers to occur from CFS to HPSS on a scheduled basis, after users have completed scanning during their alotted beamtime.

### Transfer to HPSS Implementation
**`orchestration/transfer_controller.py`: `CFSToHPSSTransferController()`**

Input
Output

### Transfer to CFS Implementation
**`orchestration/transfer_controller.py`: `HPSSToCFSTransferController()`**

Input
Output

## Update SciCat with HPSS file paths

TBD


