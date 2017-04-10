-- Análisis de la función tiger.normalize_address() y posibles modificaciones para tiger.ar_normalize_address().

-- Se modifican las expresiones para evaluar los resultados de la función,
-- ya que no pueden declararse variables en este entorno.

-- rawInput	--> '777 Massachusetts Avenue Cambridge MA 02139'
-- ws 		--> E'[ ,.\t\n\f\r]'; (regex para separadores)

-- Assume that the address begins with a digit, and extract it from the input string.
select substring('777 Massachusetts Avenue, Cambridge, MA 02139' from E'^([0-9].*?)[ ,/.]');
-- Modificando la expresión regular para que no busque sólo al principio...
select substring('Massachusetts Avenue 777, Cambridge MA 02139' from E'([0-9].*?)[ ,/.]');

-- There are two formats for zip code, the normal 5 digit , and the nine digit zip-4. It may also not exist.
select substring('777 Massachusetts Avenue Cambridge MA 02139' from E'[ ,.\t\n\f\r]' || E'([0-9]{5})$');
-- Modificando la expresión regular para que evalúe 4 dígitos (luego ver cómo evaluar CPA completo).
select substring('Buenos Aires 1425' from E'[ ,.\t\n\f\r]' || E'([0-9]{4})');
-- Formato CP alfanumérico, ej. C1425ABC.
select substring('Buenos Aires C1425ABC' from E'[ ,.\t\n\f\r]' || E'([a-zA-Z][0-9]{4}[a-zA-Z]{3})');

-- Remueve el ZIP de la dirección.
select substring('777 Massachusetts Avenue, Cambridge, MA 02139' from '(.*)' || E'[ ,.\t\n\f\r]' || '+' || cull_null('02139') || '[- ]?([0-9]{4})?$');

-- State parsing/matching | Usar tabla de datos para ARG.
select state_extract('777 Massachusetts Avenue, Cambridge, MA');
select split_part('MA:MA', ':', 1);

