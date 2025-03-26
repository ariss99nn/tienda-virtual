from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from bson.objectid import ObjectId

console = Console()

def run(db):
    """Módulo de operaciones CRUD"""
    console.print(Panel.fit("📝 [bold cyan]Operaciones CRUD en MongoDB[/bold cyan] 📝"))
    
    # Seleccionar colección
    collections = db.list_collection_names()
    if not collections:
        console.print("\ni️ No hay colecciones en esta base de datos. Creando una nueva...")
        collection_name = "ejemplo_crud"
        db.create_collection(collection_name)
    else:
        collection_name = console.input(f"\nIngrese el nombre de la colección ({', '.join(collections)}): ")
        collection = db[collection_name]

    while True:
        # Mostrar submenú CRUD
        table = Table(title="Operaciones CRUD", show_header=True)
        table.add_column("Opción", style="cyan")
        table.add_column("Operación", style="green")
        table.add_column("Descripción", style="white")
        table.add_row("1", "Insertar", "Agregar nuevos documentos")
        table.add_row("2", "Buscar", "Consultar documentos")
        table.add_row("3", "Actualizar", "Modificar documentos")
        table.add_row("4", "Eliminar", "Borrar documentos")
        table.add_row("5", "Conteo", "Contar documentos")
        table.add_row("0", "Volver", "Regresar al menú principal")
        console.print(table)
        
        choice = console.input("\n🔹Seleccione una operación CRUD (0-5): ")
        
        if choice == "0":
            break
        
        elif choice == "1":
            # Insertar documentos
            console.print("\n[bold]Insertar documentos[/bold]")
            console.print("1. Insertar uno\n2. Insertar varios")
            insert_choice = console.input("Seleccione opción (1-2): ")
            
            if insert_choice == "1":
                doc = {}
                while True:
                    key = console.input("Ingrese clave (o dejar vacío para terminar): ")
                    if not key:
                        break
                    value = console.input(f"Ingrese valor para la clave '{key}': ")
                    doc[key] = value
                
                collection.insert_one(doc)
                console.print(f"\n✅ [green]Documento insertado: {doc}[/green]")

            elif insert_choice == "2":
                docs = []
                while True:
                    doc = {}
                    while True:
                        key = console.input("Ingrese clave (o dejar vacío para terminar): ")
                        if not key:
                            break
                        value = console.input(f"Ingrese valor para la clave '{key}': ")
                        doc[key] = value
                    
                    if doc:
                        docs.append(doc)
                    
                    add_more = console.input("¿Desea agregar otro documento? (s/n): ")
                    if add_more.lower() != 's':
                        break
                
                collection.insert_many(docs)
                console.print(f"\n✅ [green]Documentos insertados: {docs}[/green]")

        elif choice == "2":
            # Buscar documentos
            console.print("\n[bold]Buscar documentos[/bold]")
            query = {}
            field = console.input("Ingrese el campo de búsqueda (o dejar vacío para buscar todo): ")
            if field:
                value = console.input(f"Ingrese el valor para '{field}': ")
                query[field] = value
            
            documents = collection.find(query)
            result_table = Table(title="Resultados de Búsqueda")
            result_table.add_column("ID", style="cyan")
            result_table.add_column("Documento", style="white")
            
            for doc in documents:
                result_table.add_row(str(doc["_id"]), str(doc))
            console.print(result_table)

        elif choice == "3":
            # Actualizar documentos
            console.print("\n[bold]Actualizar documentos[/bold]")
            query = {}
            field = console.input("Ingrese el campo de búsqueda para actualizar (o dejar vacío para actualizar todo): ")
            if field:
                value = console.input(f"Ingrese el valor para '{field}': ")
                query[field] = value
            
            update_field = console.input("Ingrese el campo a actualizar: ")
            update_value = console.input(f"Ingrese el nuevo valor para '{update_field}': ")
            
            result = collection.update_many(query, {"$set": {update_field: update_value}})
            console.print(f"\n✅ [green]Documentos actualizados: {result.modified_count}[/green]")

        elif choice == "4":
            # Eliminar documentos
            console.print("\n[bold]Eliminar documentos[/bold]")
            query = {}
            field = console.input("Ingrese el campo de búsqueda para eliminar (o dejar vacío para eliminar todo): ")
            if field:
                value = console.input(f"Ingrese el valor para '{field}': ")
                query[field] = value
            
            result = collection.delete_many(query)
            console.print(f"\n✅ [green]Documentos eliminados: {result.deleted_count}[/green]")

        elif choice == "5":
            # Conteo de documentos
            count = collection.count_documents({})
            console.print(f"\n📊 [bold]Número total de documentos: {count}[/bold]")

        else:
            console.print("\n❌[red]Opción inválida. Intente nuevamente.[/red]")
        
        console.input("\nPresione Enter para continuar...")
        console.clear()