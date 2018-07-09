## Install and Start TapData

- First you need get tapdata.tar.gz,put it in your own folder.For example:
    ```
        mkdir /opt/tapdata
        mv tapdata.tar.gz /opt/tapdata/
    ```
    
- Check ulimit
    ```
        ulimit -n
    ```
    If ulimit -n < 32768,run this
    ```
        ulimit -SHn 51200
    ```
- Decompression
    ```
        tar -zxvf tapdata.tar.gz
    ```
- Start/Restart tapdata
    ```
        ./bin/tapdata rs
    ```
- Set http port
    ```
        vi etc/sdc.properties
    ```
    Edit http.port(default port:18630),then restart tapdata
    ```
        ./bin/tapdata rs
    ```