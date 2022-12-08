import requests, json
import time, socket

class Hint:
        def __init__(self, typeVar = "memory", spaceRequiredInMegaBytes = "1000", jobName = "Job 1", replicationFactor = "5"):
                self.type= typeVar
                self.space = spaceRequiredInMegaBytes
                self.jobName = jobName
                self.replicationFactor = replicationFactor
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

class Connection:

        def __init__(self, hint, controllerUrl):
                self.controllerUrl = controllerUrl
                self.dataNodeUrl = ""
                self.hint = hint
                self.backupUrls = []
                try:
                        self.register()
                except Exception as e:
                        raise Exception(e)

        def register(self):
                params = self.hint.__dict__
                url = self.controllerUrl + "/register"
                print(params, url)
                time.sleep(5)
                r = requests.get(url = url, params = params)
                print(r.status_code)
                if(r.status_code == 200):
                        self.dataNodeUrl = 'http://' + r.json()["ip"] + ":8080"
                        self.backupUrls = r.json()["otherIps"]
                        print(self.dataNodeUrl, self.backupUrls)
                        return r
                else:
                        raise Exception("Register not successful. Status code => "+ str(r.status_code))
                

        def open_tcp(self):
                self.socket.connect((HOST, PORT))

        def close_tcp(self):
                data = "over"
                data = data.encode('UTF-8')
                self.socket(len(data).to_bytes(2, byteorder='big'))
                self.socket.sendall(data)
                        
        def put_tcp(self, key, value):

                data = key + " " + value
                data = data.encode('UTF-8')
                (len(data).to_bytes(2, byteorder='big'))
                self.socket.sendall(len(data).to_bytes(2, byteorder = 'big'))
                self.socket.sendall(data)

                length_of_message = int.from_bytes(self.socket.recv(2), byteorder='big')
                ans = self.socket.recv(length_of_message)

        def get_tcp(self, key):

                data = key
                dat = data.encode('UTF-8')
                self.socket.sendall(len(data).to_bytes(2, byteorder='big'))
                s.sendall(data)

                length_of_message = int.from_bytes(self.socket.recv(2), byteorder = 'big')
                ans = s.recv(length_of_message).decode()
                return(ans)
                
        def put(self, key, value):
                
                params = {"key":key, "value":value}
                url = self.dataNodeUrl + "/put"
                r = requests.get(url = url, params = params)

                if(len(self.backupUrls) > 0 and r.status_code != 200):
                        self.dataNodeUrl = self.backupUrls[0]
                        del(self.backupUrls[0])
                        return self.put(key, value)
                elif(len(self.backupUrls) == 0):
                        return(False)
                else:
                        return(True)

        def get(self, key):
                params = {"key":key}
                url = self.dataNodeUrl + "/get"
                r = requests.get(url = url, params = params)
                
                if(len(self.backupUrls) > 0 and r.status_code != 200):
                        self.dataNodeUrl = self.backupUrls[0]
                        del(self.backupUrls[0])
                        return self.get(key)
                elif(len(self.backupUrls) == 0):
                        return(False)
                else:
                        data = r.json()
                        return(data)




