# FLASK-JWT

1. Generar Token

- Input:
  `$ curl -XPOST localhost:5000/api/v1/auth -H "Content-Type: application/json" -d '{"username":"test", "password": "test"}'`
    
- Output:
  ```
  $ 
  {
    "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MDE4NjAwODMsImlhdCI6MTQ5OTI2ODA4MywibmJmIjoxNDk5MjY4MDgzLCJpZGVudGl0eSI6MX0.8_AmMBuKu58lIA_XzxggHrUOBDKV7Cj2bH_8rCLy2_c"
  }
  ```
  
  
2. Ingresar

- Sin token:
    `$ curl localhost:5000/api/v1/protected_stuff`


- Con token:
  ```
  $ curl localhost:5000/api/v1/protected_stuff -H "Authorization: JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MDE4NTk1NzAsImlhdCI6MTQ5OTI2NzU3MCwibmJmIjoxNDk5MjY3NTcwLCJpZGVudGl0eSI6MX0.0rICwE-xIlywf9bzf5utvsPB-Rnze8c4WATdpxUDwDg"
  ```
