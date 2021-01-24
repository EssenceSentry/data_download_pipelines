# Data download pipelines

A set of utilities for declarative specification of data download pipelines for ETL jobs. Great care was taken not to add external dependencies, at this moment it only requires `paramiko` to manage SSH connections.

At the moment, these utilities are focused on retrieving data from FTP servers or from remote machines using SSH. 
It is also possible to download files from a URL without authentication.

### A few examples

Suppose that we have a remote machine in which, inside a path, there are files named by date, and we want the data from 
the files between a `start_date` and an `end_date`.

```python
ssh = SSHConnection(host="ec2-14552256.compute-1.amazonaws.com",
                    username="ec2-user",
                    key_filename=ssh_key_file)
path = "/home/ec2-user/data/"
result = (path
            | contents(ssh)
            | filter(lambda date: start_date <= (date | date_from_str("%Y-%m-%d.xml")) <= end_date)
            | warn_if_not_found
            | map(lambda name: os.path.join(path, name))
            | map(download(self.ssh))
            | map(parse_xml(tag="users"))
            | concat)
```

Now suppose we have an FTP server and we want data from a range of dates from a single file:

```python
ftp = FTPConnection(host=config.get("host"),
                    username=config.get("username"),
                    password=config.get("password"))
result = ("data.csv"
            | download(ftp)
            | parse_csv
            | filter(
                get("Date") | date_from_str("%Y-%m-%d")
                | Pipe(lambda date: start_date <= date <= end_date))
            | warn_if_not_found)
```

There are cases where there might be files with some duplicated information that we want to filter out:

```python
result = ([file1, file2, file3]
            | map(download(ftp))
            | map(parse_csv)
            | join_if_different_ids(id_column="DataID"))
```

In other cases, the data can be compressed:

```python
result = (path
            | contents(ftp)
            | filter(
                split("/") | get("-1") | strip(r"\D+") | date_from_str("%m%d%Y")
                | Pipe(lambda date: start_date <= date <= end_date))
            | warn_if_not_found
            | map(download(ftp))
            | map(ungzip)
            | concat
            | map(parse_json)
            | concat)
```

Or even password protected:

```python
result = (path("recent")
           | download
           | unzip(password=config.get("password"))
           | concat
           | map(parse_csv(delimiter="|"))
           | filter(
               get("CREATED ON") 
               | capitalize
               | date_from_str("%d-%b-%Y %I:%M %p")
               | Pipe(lambda date: start_date <= date <= end_date))
          | warn_if_no_found)
```


There is still a lot to be done. Pull requests or feature requests are welcome.
