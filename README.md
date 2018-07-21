# INF564 Tarea 3

## Compilación
Para compilar ejecutar sbt, luego:

    compile
    package

## Ejecución
Se puede ejecutar el código directamente desde `sbt` con:
    
    run [filename.csv] [k] [EPS] [MinPts]

O directamente del paquete:
    
    spark-submit target/scala-2.11/dsnn_2.11-1.0.jar [filename.csv] [k] [EPS] [MinPts]

Si no es envian argumentos el programa funcionará con los parametros por defecto, es decir su ejecución será equivalente a :
    
    run bitcoinalpha.csv 10 3 3
