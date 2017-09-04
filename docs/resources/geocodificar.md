# Geocodificación

## Validaciones

1. Altura inexistente
    - Entorno: API
    - Validación: Altura inicial y final tienen valor cero (ambas)
    - Resultado: Texto (info)
    - Mensaje devuelto: "La calle no tiene numeración en la base de datos."


2. Inconsistencia en los datos de origen
    - Entorno: API
    - Validación: La altura inicial y final son iguales
    - Resultado: Texto (info)
    - Mensaje devuelto: "No se pudo realizar la interpolación."


3. Altura buscada fuera del rango
    - Entorno: API 
    - Validación: La altura buscada es menor a la altura inicial o mayor a la altura final
    - Resultado: Texto (info)
    - Mensaje devuelto: "La altura buscada está fuera del rango conocido."


4. Los tramos no se pueden unir (inconexos)
    - Entorno: Base de datos
    - Validación: La vía contiene tramos que no se pueden unir: no se puede georeferenciar. 
    - Resultado: json
    - Valor: `{ cod: 0, ret: "Líneas discontinuas" }` 
    - Mensaje devuelto: "La altura buscada no puede ser georeferenciada."


5. Errores inesperados
    - Entorno: Base de datos
    - Validación: Se produce un error inesperado en el motor de la base datos
    - Resutado: json
    - Valor: `{ cod: 2, ret: <PostgreSQL Error> }`

## Función geocodificar

**Parámetros**:
  - Geometría (vía de circulación con tramos conexos)
  - Altura búscada
  - Altura inicial (derecha)
  - Altura final  (izquierda)

**Retorna**:
  - Json  
  - Valor: `{ cod: 1, ret: <Coordenadas(lat, lng)> }`
  - Mensaje devuelto: "Se procesó correctamente la dirección buscada."
