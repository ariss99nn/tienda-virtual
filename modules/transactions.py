from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from pymongo import MongoClient
import random

console = Console()

def run(db):
    """Módulo de Transacciones en MongoDB"""
    console.print(Panel.fit("🔄 [bold cyan]Transacciones en MongoDB[/bold cyan] 🔄"))

    # Crear colecciones de ejemplo (cuentas y movimientos)
    if "cuentas" not in db.list_collection_names():
        console.print("\nℹ️ Creando colecciones 'cuentas' y 'movimientos'...")
        create_sample_transaction_data(db)

    while True:
        table = Table(title="Operaciones Transaccionales", show_header=True)
        table.add_column("Opción", style="cyan")
        table.add_column("Operación", style="green")
        table.add_column("Descripción", style="white")

        table.add_row("1", "Transferencia simple", "Mover dinero entre cuentas (sin transacción)")
        table.add_row("2", "Transferencia transaccional", "Mover dinero con transacción ACID")
        table.add_row("3", "Transacción fallida", "Simular error y rollback")
        table.add_row("4", "Ver estado cuentas", "Mostrar saldos actuales")
        table.add_row("0", "Volver", "Regresar al menú principal")

        console.print(table)

        choice = console.input("\n🔹 Seleccione una operación (0-4): ")

        if choice == "0":
            break

        elif choice == "1":
            console.print("\n[bold]Transferencia SIN Transacción:[/bold]")
            from_acc = console.input("Cuenta origen (1-5): ")
            to_acc = console.input("Cuenta destino (1-5): ")
            amount = float(console.input("Monto a transferir: "))

            try:
                # Operaciones individuales (sin transacción)
                db["cuentas"].update_one(
                    {"cuenta_id": int(from_acc)},
                    {"$inc": {"balance": -amount}}
                )
                db["cuentas"].update_one(
                    {"cuenta_id": int(to_acc)},
                    {"$inc": {"balance": amount}}
                )

                # Registrar movimiento
                db["movimientos"].insert_one({
                    "tipo": "transferencia",
                    "origen": int(from_acc),
                    "destino": int(to_acc),
                    "monto": amount,
                    "estado": "completado"
                })

                console.print("\n✅ [green]Transferencia completada (sin transacción)[/green]")
            except Exception as e:
                console.print(f"\n❌ [red]Error: {e}[/red]")

        elif choice == "2":
            console.print("\n[bold]Transferencia CON Transacción:[/bold]")
            from_acc = console.input("Cuenta origen (1-5): ")
            to_acc = console.input("Cuenta destino (1-5): ")
            amount = float(console.input("Monto a transferir: "))

            session = db.client.start_session()
            try:
                with session.start_transaction():
                    # Verificar fondos
                    cuenta_origen = db["cuentas"].find_one(
                        {"cuenta_id": int(from_acc)},
                        session=session
                    )
                    if cuenta_origen["balance"] < amount:
                        raise ValueError("Fondos insuficientes")

                    # Actualizar cuentas
                    db["cuentas"].update_one(
                        {"cuenta_id": int(from_acc)},
                        {"$inc": {"balance": -amount}},
                        session=session
                    )
                    db["cuentas"].update_one(
                        {"cuenta_id": int(to_acc)},
                        {"$inc": {"balance": amount}},
                        session=session
                    )

                    # Registrar movimiento
                    db["movimientos"].insert_one({
                        "tipo": "transferencia",
                        "origen": int(from_acc),
                        "destino": int(to_acc),
                        "monto": amount,
                        "estado": "completado"
                    }, session=session)

                    session.commit_transaction()
                    console.print("\n✅ [green]Transacción completada con éxito[/green]")

            except Exception as e:
                session.abort_transaction()
                console.print(f"\n❌ [red]Transacción fallida (rollback): {e}[/red]")
            finally:
                session.end_session()

        elif choice == "3":
            console.print("\n[bold]Simular Transacción Fallida:[/bold]")
            console.print("Se transferirá $100 pero se forzará un error")

            session = db.client.start_session()
            try:
                with session.start_transaction():
                    # Transferencia válida
                    db["cuentas"].update_one(
                        {"cuenta_id": 1},
                        {"$inc": {"balance": -100}},
                        session=session
                    )
                    db["cuentas"].update_one(
                        {"cuenta_id": 2},
                        {"$inc": {"balance": 100}},
                        session=session
                    )

                    # Forzar error
                    raise ValueError("Error simulado en la transacción")

                    # Esto no se ejecutará
                    session.commit_transaction()

            except Exception as e:
                session.abort_transaction()
                console.print(f"\n❌ [red]Transacción fallida (esperado): {e}[/red]")
                console.print("✅ [green]Se realizó rollback automático[/green]")
            finally:
                session.end_session()

        elif choice == "4":
            console.print("\n[bold]Estado Actual de Cuentas:[/bold]")
            cuentas = list(db["cuentas"].find().sort("cuenta_id", 1))

            table = Table(title="Saldos de Cuentas", show_header=True)
            table.add_column("Cuenta ID")
            table.add_column("Titular")
            table.add_column("Balance")

            for cuenta in cuentas:
                table.add_row(
                    str(cuenta["cuenta_id"]),
                    cuenta["titular"],
                    f"${cuenta['balance']:.2f}"
                )

            console.print(table)

            # Mostrar últimos movimientos
            movimientos = list(db["movimientos"].find().sort("_id", -1).limit(5))
            if movimientos:
                console.print("\n[bold]Últimos 5 movimientos:[/bold]")
                mov_table = Table(show_header=True)
                mov_table.add_column("Fecha")
                mov_table.add_column("Tipo")
                mov_table.add_column("Detalle")
                mov_table.add_column("Monto")

                for mov in movimientos:
                    detalle = f"{mov.get('origen', '')} → {mov.get('destino', '')}"
                    mov_table.add_row(
                        str(mov["_id"].generation_time),
                        mov["tipo"],
                        detalle,
                        f"${mov['monto']:.2f}"
                    )

                console.print(mov_table)

        else:
            console.print("\n❌ [red]Opción inválida. Intente nuevamente.[/red]")

        console.input("\nPresione Enter para continuar...")
        console.clear()

def create_sample_transaction_data(db):
    """Crea datos de ejemplo para transacciones"""
    # Crear cuentas bancarias
    cuentas = [
        {"cuenta_id": 1, "titular": "Juan Pérez", "balance": 1000.0},
        {"cuenta_id": 2, "titular": "María Gómez", "balance": 1500.0},
        {"cuenta_id": 3, "titular": "Empresa XYZ", "balance": 5000.0},
        {"cuenta_id": 4, "titular": "Carlos Ruiz", "balance": 750.0},
        {"cuenta_id": 5, "titular": "Ana López", "balance": 3000.0}
    ]
    db["cuentas"].insert_many(cuentas)

    # Crear colección de movimientos vacía
    db.create_collection("movimientos")

    console.print("✅ [green]Colecciones 'cuentas' y 'movimientos' creadas[/green]")