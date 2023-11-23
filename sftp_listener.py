import pysftp
import time

def main():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    hostname = "192.168.1.76"
    username = "afonsob"
    password = "300798ab"
    port = 22

    remote_directory = "/home/bate/smart-retail-example/V1/ssh-test"
    local_directory = "/home/bate/smart-retail-example/V1/output/state_store"

    while True:
        try:
            sftp = pysftp.Connection(host=hostname, username=username, password=password, port=port, cnopts=cnopts)
            remote_files = sftp.listdir(remote_directory)
            for filename in remote_files:
                remote_file_path = f"{remote_directory}/{filename}"
                local_file_path = f"{local_directory}/{filename}"

                sftp.get(remote_file_path, local_file_path)
                print(f"Downloaded: {remote_file_path}")
                sftp.remove(remote_file_path)
                sftp.close()

        except Exception as e:
            print("Nao liguei")
            #print(f"Error: {e}")
        time.sleep(1)

if __name__ == "__main__":
    main()