# JWT

- Estándar abierto basado en JSON
- Principal ventaja es que se almacena en el localstorage del cliente.

## Caso de uso estandar
             
    [usuario] -> datos de inicio de sesion -> [servidor] 
    [usuario] <- se la envia a la app cliente <-  *genera JWT*
    [usuario] <- en cada peticion envia este token [servidor]
    
## Estructura: constituida por tres partes/strings

- header: string en base64
- payload: string en base64
- signature: toma las dos partes anteriores y las encripta usando el algoritmo: SHA-256

	```json
		{
  		"alg": "HS256",
  		"typ": "JWT"
		}
  ```
  
### Payload: 
Json que puede tener cualquier propiedad que le queramos añadir.

#### Propiedades posibles

- Tipo de token(typ)
- Tipo de contenido(cty)
- Algoritmo de firmado(alg)
- Cualquier otro incluido

#### Propiedades estandar

- Creador(iss): quien creo el JWT.
- Razon(sub): Identifica la razon de JWT.
- Audencia(aud): Quien se supone que va a recibir el JWT(web, android, ios).
- Tipo de expiracion(exp). Obliga al usuario volver a autentificarse.
- No antes(nbf). Identifica desde que momento se va a empezar a aceptar un JWT.
- Creado(iat). Cuando fue creado el JWT.
- ID(jti). Identificador unico por cada JWT.

### Signature
Es la firma de JWT generada apartir de los campos header y payload (base64) y una key secreta.

```
		key =  'secret'
		unsignedToken = base64Encode(header) + '.' + base64Encode(payload)
		signature = SHA256(key, unsignedToken)
		token = unsignedToken + '.' + signature
```

#### Links utiles
[Introducción](https://platzi.com/blog/introduccion-json-web-tokens/)
[Flask JWT](https://pythonhosted.org/Flask-JWT/)
[Vulneravilidades](https://auth0.com/blog/critical-vulnerabilities-in-json-web-token-libraries/)
[Configuración](https://support.zendesk.com/hc/es/articles/203663816-Configuraci%C3%B3n-de-inicio-de-sesi%C3%B3n-%C3%BAnico-con-JWT-Token-Web-JSON-)
[Tutorial](https://realpython.com/blog/python/token-based-authentication-with-flask/)
