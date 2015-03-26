Procesamiento paralelo de registros de accidentes y multas en la ciudad de Montevideo
-------------------------------------------------------------------------------------

Proyecto de la materia Computación de Alta Performance de la Facultad de Ingeniería, UdelaR.

Autores: Lesly Acuña y Renzo Massobrio
Contacto: lesly.acuna@fing.edu.uy | renzom@fing.edu.uy

-------------------------------------------------------------------------------------
Visualización de los mapas:
**************************

Los mapas con los resultados del procesamiento se encuentran en las carpetas ACCIDENTES/Resultados y MULTAS/Resultados.


Instrucciones de ejecución:
**************************

*Cambiar la cantidad de hilos a utilizar en los archivos procesar_multas.py y procesar_accidentes.py de acuerdo a la cantidad de procesadores disponibles.
*Ejecutar con el siguiente comando:
	python procesar_multas.py
o
	python procesar_accidentes.py
*Para generar los mapas:
	-Instalar el software QGIS (www.qgis.org)
	-Arrastrar el archivo barrios_wgs_84.shp para ver el mapa de montevideo
	-Ir a Layers -> Add delimited text layer, abrir el archivo resultados_accidentes.csv o resultados_multas.csv que se crea luego de la ejecución de los scripts python, y seleccionar la opción No Geometry.
	-Ir a las propiedades de la capa correspondiente al Mapa a la opción "Joins"
	-Agregar un nuevo Join con la capa correspondiente a los resultados utilizando el atributo NRO_BARRIO
	-En las propiedades de la capa del mapa, ir a Style y cambiar la opción Single Symbol por Graduated, en la opción Column elegir el segundo campo de la capa resultados. Elegir una paleta de colores, y la cantidad de clases en que separar los valores.
------------------------------------------------------------------------------------
 