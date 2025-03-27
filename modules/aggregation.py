from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from bson.json_util import dumps
import json

console = Console()

def run(db):
    """Módulo de Pipeline de Agregación en MongoDB"""
    console.print(Panel.fit("🔢 [bold cyan]Agregaciones en MongoDB[/bold cyan] 🔢"))

    # Crear colección de ejemplo (ventas)
    if "ventas" not in db.list_collection_names():
        console.print("\nℹ️ Creando colección 'ventas' con datos de ejemplo...")
        create_sample_sales_data(db)

    collection = db["ventas"]

    while True:
        table = Table(title="Operaciones de Agregación", show_header=True)
        table.add_column("Opción", style="cyan")
        table.add_column("Operación", style="green")
        table.add_column("Descripción", style="white")

        table.add_row("1", "Agregación básica", "Ejemplo: Conteo y suma de ventas")
        table.add_row("2", "Agrupación por campo", "Agrupar por categoría/producto")
        table.add_row("3", "Filtros en pipelines", "Filtrar antes de agrupar")
        table.add_row("4", "Operadores avanzados", "Uso de $lookup, $unwind")
        table.add_row("5", "Pipeline personalizado", "Escribir tu propio pipeline")
        table.add_row("0", "Volver", "Regresar al menú principal")

        console.print(table)

        choice = console.input("\n🔹 Seleccione una operación (0-5): ")

        if choice == "0":
            break

        elif choice == "1":
            console.print("\n[bold]Agregación Básica:[/bold] Conteo y total de ventas")
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "total_ventas": {"$sum": "$monto"},
                        "cantidad_ventas": {"$sum": 1}
                    }
                }
            ]
            print_aggregation(collection, pipeline)

        elif choice == "2":
            console.print("\n[bold]Agrupación por Categoría:[/bold] Ventas por producto")
            pipeline = [
                {
                    "$group": {
                        "_id": "$producto",
                        "total": {"$sum": "$monto"},
                        "promedio": {"$avg": "$monto"},
                        "ventas": {"$sum": 1}
                    }
                },
                {"$sort": {"total": -1}}
            ]
            print_aggregation(collection, pipeline)

        elif choice == "3":
            console.print("\n[bold]Filtros en Pipeline:[/bold] Ventas > $100 en 2023")
            pipeline = [
                {
                    "$match": {
                        "fecha": {"$gte": "2023-01-01", "$lte": "2023-12-31"},
                        "monto": {"$gt": 100}
                    }
                },
                {
                    "$group": {
                        "_id": "$producto",
                        "total": {"$sum": "$monto"}
                    }
                }
            ]
            print_aggregation(collection, pipeline)

        elif choice == "4":
            console.print("\n[bold]Operadores Avanzados ($lookup):[/bold] Unir con colección 'productos'")
            
            # Crear colección productos si no existe
            if "productos" not in db.list_collection_names():
                db["productos"].insert_many([
                    {"nombre": "Laptop", "categoria": "Tecnología"},
                    {"nombre": "Smartphone", "categoria": "Tecnología"},
                    {"nombre": "Camisa", "categoria": "Ropa"}
                ])

            pipeline = [
                {
                    "$lookup": {
                        "from": "productos",
                        "localField": "producto",
                        "foreignField": "nombre",
                        "as": "info_producto"
                    }
                },
                {"$unwind": "$info_producto"},
                {
                    "$group": {
                        "_id": "$info_producto.categoria",
                        "ventas_totales": {"$sum": "$monto"}
                    }
                }
            ]
            print_aggregation(collection, pipeline)

        elif choice == "5":
            console.print("\n[bold]Pipeline Personalizado:[/bold] Escribe tu propio pipeline")
            console.print("Ejemplo: [{'$match': {'producto': 'Laptop'}}, {'$count': 'total'}]")
            try:
                pipeline_input = console.input("Ingresa el pipeline (formato JSON): ")
                pipeline = json.loads(pipeline_input)
                print_aggregation(collection, pipeline)
            except Exception as e:
                console.print(f"\n❌ [red]Error: {e}[/red]")

        else:
            console.print("\n❌ [red]Opción inválida. Intente nuevamente.[/red]")

        console.input("\nPresione Enter para continuar...")
        console.clear()

def print_aggregation(collection, pipeline):
    """Ejecuta y muestra resultados de un pipeline de agregación"""
    console.print("\n[bold]Pipeline ejecutado:[/bold]")
    console.print(dumps(pipeline, indent=2))

    try:
        results = list(collection.aggregate(pipeline))
        
        if not results:
            console.print("\nℹ️ No se encontraron resultados")
            return

        # Crear tabla con los resultados
        table = Table(title="Resultados de Agregación", show_header=True)
        
        # Obtener las claves del primer documento para las columnas
        for key in results[0].keys():
            table.add_column(str(key))

        for doc in results:
            table.add_row(*[str(doc.get(key, "")) for key in doc.keys()])

        console.print(table)
    except Exception as e:
        console.print(f"\n❌ [red]Error en agregación: {e}[/red]")

def create_sample_sales_data(db):
    """Crea datos de ejemplo para el módulo de agregación"""
    ventas = []
    productos = ["Laptop", "Smartphone", "Camisa", "Zapatos", "Libro"]
    
    for i in range(1, 101):
        venta = {
            "producto": random.choice(productos),
            "monto": round(random.uniform(10, 500), 2),
            "fecha": f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "vendedor": f"Vendedor-{random.randint(1, 5)}",
            "metodo_pago": random.choice(["Tarjeta", "Efectivo", "Transferencia"])
        }
        ventas.append(venta)
    
    db["ventas"].insert_many(ventas)
    console.print(f"✅ [green]Insertadas {len(ventas)} ventas de ejemplo[/green]")