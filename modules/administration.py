from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress
import subprocess
import os
from datetime import datetime

console = Console()

def run(db):
    """Módulo de administración de MongoDB"""
    console.print(Panel.fit("⚙️ [bold cyan]Administración de MongoDB[/bold cyan] ⚙️"))
    
    while True:
        table = Table(title="Operaciones de Administración", show_header=True)
        table.add_column("Opción", style="cyan")
        table.add_column("Operación", style="green")
        table.add_column("Descripción", style="white")
        
        table.add_row("1", "Estadísticas del servidor", "Ver información del servidor MongoDB")
        table.add_row("2", "Gestión de usuarios", "Crear y administrar usuarios")
        table.add_row("3", "Copia de seguridad (mongodump)", "Exportar datos a archivo")
        table.add_row("4", "Restaurar (mongorestore)", "Importar datos desde archivo")
        table.add_row("5", "Exportar colección (mongoexport)", "Exportar colección a JSON/CSV")
        table.add_row("6", "Importar colección (mongoimport)", "Importar datos a colección")
        table.add_row("0", "Volver", "Regresar al menú principal")
        
        console.print(table)
        
        choice = console.input("\n🔹 Seleccione una operación (0-6): ")
        
        if choice == "0":
            break
            
        elif choice == "1":
            console.print("\n[bold]Estadísticas del servidor:[/bold]")
            try:
                # Información del servidor
                server_info = db.command("serverStatus")
                
                # Información de la base de datos actual
                db_stats = db.command("dbStats")
                
                # Mostrar información básica
                info_table = Table(title="Información del Servidor", show_header=False)
                info_table.add_column("Campo")
                info_table.add_column("Valor")
                
                info_table.add_row("Host", f"{db.client.HOST}:{db.client.PORT}")
                info_table.add_row("Versión MongoDB", server_info["version"])
                info_table.add_row("Motor de almacenamiento", server_info["storageEngine"]["name"])
                info_table.add_row("Tiempo de actividad", f"{server_info['uptime'] / 3600:.2f} horas")
                info_table.add_row("Base de datos actual", db.name)
                info_table.add_row("Tamaño de datos", f"{db_stats['dataSize'] / (1024*1024):.2f} MB")
                info_table.add_row("Almacenamiento usado", f"{db_stats['storageSize'] / (1024*1024):.2f} MB")
                
                console.print(info_table)
                
                # Operaciones actuales
                current_ops = db.current_op()
                if current_ops["inprog"]:
                    ops_table = Table(title="Operaciones en curso", show_header=True)
                    ops_table.add_column("ID")
                    ops_table.add_column("Tipo")
                    ops_table.add_column("Colección")
                    ops_table.add_column("Tiempo (ms)")
                    
                    for op in current_ops["inprog"]:
                        ops_table.add_row(
                            str(op["opid"]),
                            op.get("op", "?"),
                            op.get("ns", "?"),
                            str(op.get("microsecs_running", 0) // 1000)
                        )
                    
                    console.print(ops_table)
                else:
                    console.print("\nℹ️ No hay operaciones en curso")
                    
            except Exception as e:
                console.print(f"\n❌ [red]Error al obtener estadísticas: {e}[/red]")
                
        elif choice == "2":
            console.print("\n[bold]Gestión de usuarios[/bold]")
            console.print("1. Listar usuarios\n2. Crear usuario\n3. Eliminar usuario")
            user_choice = console.input("Seleccione opción (1-3): ")
            
            admin_db = db.client["admin"]
            
            if user_choice == "1":
                try:
                    users = list(admin_db.command("usersInfo")["users"])
                    
                    if users:
                        user_table = Table(title="Usuarios del Sistema", show_header=True)
                        user_table.add_column("Usuario")
                        user_table.add_column("Roles")
                        
                        for user in users:
                            roles = ", ".join([f"{r['role']}@{r['db']}" for r in user["roles"]])
                            user_table.add_row(user["user"], roles)
                            
                        console.print(user_table)
                    else:
                        console.print("\nℹ️ No hay usuarios definidos")
                except Exception as e:
                    console.print(f"\n❌ [red]Error al listar usuarios: {e}[/red]")
                    
            elif user_choice == "2":
                try:
                    username = console.input("Nombre de usuario: ")
                    password = console.input("Contraseña: ", password=True)
                    roles_input = console.input("Roles (ej: 'readWrite@db1,read@db2'): ")
                    
                    roles = []
                    for role in roles_input.split(","):
                        role_parts = role.split("@")
                        if len(role_parts) == 2:
                            roles.append({"role": role_parts[0].strip(), "db": role_parts[1].strip()})
                    
                    admin_db.command("createUser", username, pwd=password, roles=roles)
                    console.print("\n✅ [green]Usuario creado correctamente[/green]")
                except Exception as e:
                    console.print(f"\n❌ [red]Error al crear usuario: {e}[/red]")
                    
            elif user_choice == "3":
                try:
                    username = console.input("Nombre de usuario a eliminar: ")
                    admin_db.command("dropUser", username)
                    console.print("\n✅ [green]Usuario eliminado correctamente[/green]")
                except Exception as e:
                    console.print(f"\n❌ [red]Error al eliminar usuario: {e}[/red]")
                    
        elif choice == "3":
            console.print("\n[bold]Copia de seguridad (mongodump)[/bold]")
            output_dir = console.input("Directorio de salida [./backup]: ") or "./backup"
            
            try:
                with Progress() as progress:
                    task = progress.add_task("[cyan]Realizando copia de seguridad...", total=100)
                    
                    # Comando mongodump
                    cmd = [
                        "mongodump",
                        f"--host={db.client.HOST}:{db.client.PORT}",
                        f"--db={db.name}",
                        f"--out={output_dir}"
                    ]
                    
                    # Ejecutar comando
                    process = subprocess.Popen(
                        cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE
                    )
                    
                    # Simular progreso (en una aplicación real monitorearíamos el proceso)
                    while process.poll() is None:
                        progress.update(task, advance=1)
                        time.sleep(0.1)
                    
                    # Completar progreso
                    progress.update(task, completed=100)
                    
                    # Verificar resultado
                    if process.returncode == 0:
                        console.print(f"\n✅ [green]Copia de seguridad completada en {output_dir}[/green]")
                    else:
                        error = process.stderr.read().decode()
                        console.print(f"\n❌ [red]Error en mongodump: {error}[/red]")
                        
            except Exception as e:
                console.print(f"\n❌ [red]Error al ejecutar copia de seguridad: {e}[/red]")
                
        elif choice == "4":
            console.print("\n[bold]Restaurar copia de seguridad (mongorestore)[/bold]")
            input_dir = console.input("Directorio de la copia [./backup]: ") or "./backup"
            
            try:
                with Progress() as progress:
                    task = progress.add_task("[cyan]Restaurando copia de seguridad...", total=100)
                    
                    # Comando mongorestore
                    cmd = [
                        "mongorestore",
                        f"--host={db.client.HOST}:{db.client.PORT}",
                        f"--db={db.name}",
                        input_dir
                    ]
                    
                    # Ejecutar comando
                    process = subprocess.Popen(
                        cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE
                    )
                    
                    # Simular progreso
                    while process.poll() is None:
                        progress.update(task, advance=1)
                        time.sleep(0.1)
                    
                    # Completar progreso
                    progress.update(task, completed=100)
                    
                    # Verificar resultado
                    if process.returncode == 0:
                        console.print("\n✅ [green]Restauración completada correctamente[/green]")
                    else:
                        error = process.stderr.read().decode()
                        console.print(f"\n❌ [red]Error en mongorestore: {error}[/red]")
                        
            except Exception as e:
                console.print(f"\n❌ [red]Error al ejecutar restauración: {e}[/red]")
                
        elif choice == "5":
            console.print("\n[bold]Exportar colección (mongoexport)[/bold]")
            collection_name = console.input("Nombre de la colección: ")
            output_file = console.input("Archivo de salida [./export.json]: ") or "./export.json"
            file_type = "json" if output_file.endswith(".json") else "csv"
            
            try:
                with Progress() as progress:
                    task = progress.add_task("[cyan]Exportando colección...", total=100)
                    
                    # Comando mongoexport
                    cmd = [
                        "mongoexport",
                        f"--host={db.client.HOST}:{db.client.PORT}",
                        f"--db={db.name}",
                        f"--collection={collection_name}",
                        f"--out={output_file}"
                    ]
                    
                    if file_type == "csv":
                        fields = console.input("Campos a exportar (separados por coma): ")
                        cmd.extend(["--type=csv", f"--fields={fields}"])
                    
                    # Ejecutar comando
                    process = subprocess.Popen(
                        cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE
                    )
                    
                    # Simular progreso
                    while process.poll() is None:
                        progress.update(task, advance=1)
                        time.sleep(0.1)
                    
                    # Completar progreso
                    progress.update(task, completed=100)
                    
                    # Verificar resultado
                    if process.returncode == 0:
                        console.print(f"\n✅ [green]Exportación completada en {output_file}[/green]")
                    else:
                        error = process.stderr.read().decode()
                        console.print(f"\n [red]Error en mongoexport: {error}[/red]")
                        
            except Exception as e:
                console.print(f"\n❌ [red]Error al exportar colección: {e}[/red]")
                
        elif choice == "6":
            console.print("\n[bold]Importar datos (mongoimport)[/bold]")
            input_file = console.input("Archivo a importar: ")
            collection_name = console.input("Nombre de la colección destino: ")
            file_type = "json" if input_file.endswith(".json") else "csv"
            
            try:
                with Progress() as progress:
                    task = progress.add_task("[cyan]Importando datos...", total=100)
                    
                    # Comando mongoimport
                    cmd = [
                        "mongoimport",
                        f"--host={db.client.HOST}:{db.client.PORT}",
                        f"--db={db.name}",
                        f"--collection={collection_name}",
                        f"--file={input_file}"
                    ]
                    
                    if file_type == "csv":
                        cmd.append("--type=csv")
                        header = console.input("¿El archivo tiene encabezados? (s/n) [s]: ") or "s"
                        if header.lower() == "s":
                            cmd.append("--headerline")
                    
                    # Ejecutar comando
                    process = subprocess.Popen(
                        cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE
                    )
                    
                    # Simular progreso
                    while process.poll() is None:
                        progress.update(task, advance=1)
                        time.sleep(0.1)
                    
                    # Completar progreso
                    progress.update(task, completed=100)
                    
                    # Verificar resultado
                    if process.returncode == 0:
                        output = process.stdout.read().decode()
                        console.print(f"\n✅ [green]Importación completada:\n{output}[/green]")
                    else:
                        error = process.stderr.read().decode()
                        console.print(f"\n❌ [red]Error en mongoimport: {error}[/red]")
                        
            except Exception as e:
                console.print(f"\n❌ [red]Error al importar datos: {e}[/red]")
                
        else:
            console.print("\n❌ [red]Opción inválida. Intente nuevamente.[/red]")
            
        console.input("\nPresione Enter para continuar...")
        console.clear()