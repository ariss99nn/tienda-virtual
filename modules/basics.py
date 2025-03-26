from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

def run(db):
    """Modulo de conceptos basicos de MongoDB"""
console.print(Panel.fit("[bold cyan]Conceptos Basicos de  MongoDB[/bold cyan]"))

while True:
    #Mostrar submenu
    table = Table(title="Operaciones Basicas", show_header=True)
    table.add_column("Opcion", style="cyan")
    table.add_column("Comando",style="green")
    table.add_column("Descripcion", style="white")
    
    table.add_row("1", "db.help()", "Mostrar ayuda de comandos de base de datos")
    table.add_row("2", "db.stats()", "Mostrar estadísticas de la BD")
    table.add_row("3", "show dbs", "Listar todas las bases de datos")
    table.add_row("4", "use <db>", "Cambiar a una base de datos")
    table.add_row("5", "db.dropDatabase()", "Eliminar la base de datos actual")
    table.add_row("6", "db.createCollection()", "Crear una nueva colección")
    table.add_row("7", "show collections", "Listar colecciones en la BD actual")
    table.add_row("8", "db.<col>.drop()", "Eliminar una colección")
    table.add_row("0", "Volver","Regresar al menú principal")
    console.print(table)
    choice = console.input("\n🔹Seleccione una operación para ejecutar (0-8): ")
    if choice == "0":
        break
    
    elif choice == "1":
        console.print("\n[bold]Ejemplo de db.help():[/bold]")
        console.print("""
        Este comando muestra todos los métodos disponibles para manipular la base de datos.
        [yellow]Uso:[/yellow]
        > db.help()
        [yellow]Salida típica:[/yellow]
        DB methods:
        db.adminCommand(nameOrDocument) - switches to 'admin' db
        db.aggregate([pipeline], {options}) - performs aggregation
        db.createCollection(name, options) - creates new collection
        ... (más métodos)
        """)
    elif choice == "2":
        console.print("\n[bold]Ejemplo de db.stats():[/bold]")
        stats = db.command("dbstats")
        result_table = Table(title="Estadísticas de la Base de Datos")
        result_table.add_column("Métrica")
        result_table.add_column("Valor")
        
        for key, value in stats.items():
            result_table.add_row(str(key), str(value))
            
        console.print(result_table)
        
    elif choice == "3":
        console.print("\n[bold]Ejemplo de show dbs:[/bold]")
        dbs = db.client.list_database_names()
        
        db_table = Table(title="Bases de Datos Disponibles")
        db_table.add_column("Nombre")
        
        for db_name in dbs:
            db_table.add_row(db_name)
            
        console.print(db_table)
        
    elif choice == "4":
        db_name = console.input("Ingrese el nombre de la BD a cambiar: ")
        try:
            new_db = db.client[db_name]
            console.print(f"\n✅ [green]Cambiado a la base de datos '{db_name}'[/green]")
            # Mostrar colecciones en la nueva BD
            cols = new_db.list_collection_names()
            if cols:
                console.print(f"\nColecciones en '{db_name}':")
                for col in cols:
                    console.print(f"- {col}")
            else:
                console.print(f"\ni️ La BD '{db_name}' no tiene colecciones")
        except Exception as e:
            console.print(f"\n❌ [red]Error: {e}[/red]")
        # ... (implementar otras opciones)
    else:
        console.print("\n❌[red]Opción inválida. Intente nuevamente.[/red]")
        console.input("\nPresione Enter para continuar...")
        console.clear()