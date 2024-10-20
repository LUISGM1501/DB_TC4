# Proyecto: Mensajería con Kafka y Neo4J

## 1. Introducción

### Objetivo del proyecto
Este proyecto tiene como objetivo construir una aplicación que simule un entorno de comercio electrónico utilizando **Apache Kafka** para procesar en tiempo real eventos generados por usuarios (compras, interacciones con productos y conexiones sociales) y almacenarlos en una base de datos de grafos **Neo4J**. Posteriormente, se realizan consultas sobre los datos para obtener información valiosa sobre los comportamientos y conexiones de los usuarios.

### Componentes principales
El proyecto está compuesto por los siguientes componentes:
1. **Apache Kafka**: Utilizado para recibir y distribuir mensajes de eventos en tiempo real.
2. **Neo4J**: Utilizado para almacenar y analizar las relaciones entre usuarios, productos y transacciones en un formato de grafo.
3. **Productores y Consumidores de Kafka**: Implementados en Python para simular la generación de eventos y su consumo para almacenamiento en Neo4J.
4. **Consultas Cypher**: Utilizadas para analizar los datos almacenados en Neo4J.

---

## 2. Arquitectura del Proyecto

### Componentes del sistema
- **Apache Kafka**:
  - Utilizado como sistema de mensajería distribuida para permitir el flujo de datos en tiempo real.
  - Tópicos utilizados: `purchases`, `product_interactions`, `user_connections`.
  
- **Neo4J**:
  - Almacena las interacciones de los usuarios y permite realizar consultas sobre el grafo generado.
  
- **Producers** (Generadores de eventos):
  - **`PurchaseProducer`**: Simula eventos de compra de productos por parte de usuarios.
  - **`UserConnectionProducer`**: Simula conexiones sociales entre usuarios.
  - **`ProductInteractionProducer`**: Simula interacciones como reseñas o calificaciones de productos.
  
- **Consumers** (Consumidores de eventos):
  - **`PurchaseConsumer`**: Consume eventos de compra y los almacena en Neo4J.
  - **`UserConnectionConsumer`**: Consume eventos de conexiones sociales y los almacena en Neo4J.
  - **`ProductInteractionConsumer`**: Consume eventos de interacciones con productos y los almacena en Neo4J.

---

## 3. Descripción del Código

### Producers (Generadores de Eventos)
- **`product_interaction_producer.py`**:
  - Genera eventos aleatorios de interacciones con productos (reseñas o calificaciones) y los envía al tópico `product_interactions`.
  - Implementa un bucle continuo que simula la generación de eventos cada cierto tiempo aleatorio.

- **`purchase_producer.py`**:
  - Genera eventos de compras realizadas por usuarios y los envía al tópico `purchases`.
  - Los eventos incluyen información del usuario, el producto y la cantidad comprada.

- **`user_connection_producer.py`**:
  - Genera eventos que simulan que un usuario sigue a otro usuario, enviándolos al tópico `user_connections`.

### Consumers (Consumidores de Eventos)
- **`product_interaction_consumer.py`**:
  - Consume eventos de `product_interactions` y almacena las interacciones en Neo4J como relaciones `REVIEWED` o `RATED` entre usuarios y productos.
  
- **`purchase_consumer.py`**:
  - Consume eventos de `purchases` y almacena las transacciones en Neo4J utilizando nodos de `Transaction` conectados a usuarios y productos.
  
- **`user_connection_consumer.py`**:
  - Consume eventos de `user_connections` y crea relaciones `FOLLOWS` entre usuarios en Neo4J.

### Consultas Cypher
El archivo `graph_operations.py` incluye las consultas que permiten analizar los datos almacenados en Neo4J:
1. **Productos más comprados por categoría en los últimos 30 días** (`most_purchased_products_by_category`).

![consulta1]()
   
2. **Usuarios más influyentes por cantidad de seguidores** (`most_influential_users`).

![consulta2]()
   
3. **Productos mejor calificados y usuarios que los calificaron** (`best_rated_products`).

![consulta3]()
   
4. **Usuarios que compran productos similares (clusters)** (`user_product_clusters`).

![consulta4]()

---

## 4. Configuración del Entorno

### Docker Compose
`docker-compose.yml` los servicios para ejecutar el proyecto.
- **Zookeeper**: Coordina los nodos de Kafka.
- **Kafka**: Proporciona el sistema de mensajería distribuido.
- **Neo4J**: Base de datos que almacena las relaciones entre usuarios, productos y transacciones.
- **Python-App**: Ejecuta los productores y consumidores de eventos.

### Dockerfile
`Dockerfile` construye una imagen Docker.

### Main.py
Punto de entrada para la ejecución de la aplicación. Sus funciones son:
1. Esperar a que Kafka esté disponible antes de iniciar los productores y consumidores.
2. Iniciar los componentes de manera concurrente.
3. Manejar la interrupción o cierre ordenado de los servicios en caso de recibir una señal del sistema (SIGINT o SIGTERM).

---

## 5. Ejecución del Proyecto

### Instrucciones para ejecutar con Docker Compose
1. Clonar el repositorio del proyecto con el siguiente comand:
   git clone https://github.com/LUISGM1501/DB_TC4.git

2. Construir y levantar los servicios con Docker Compose:
   docker-compose up --build

3. Descargar las librerías necesarias para la conexión de python con neo4j y kafka:
   pip install kafka
   pip install neo4j
