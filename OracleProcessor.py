# ------------------------------------------------------------
# Warstwa dostępu do Oracle przy użyciu SQLAlchemy
# ------------------------------------------------------------

import os
from typing import Optional, Tuple, Dict
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class OracleProcessor:
    """Obsługa Oracle"""

    # ------------------------------------------------------------
    # ENV
    # ------------------------------------------------------------

    def get_env(self) -> dict:
        return {
            "ORACLE_USER":     str(os.getenv("ORACLE_USER")),
            "ORACLE_PASSWORD": str(os.getenv("ORACLE_PASS")),
            "ORACLE_HOST":     str(os.getenv("ORACLE_SERVER")),
            "ORACLE_PORT":     str(os.getenv("ORACLE_PORT", "1521")),
            "ORACLE_SERVICE":  str(os.getenv("ORACLE_SN")),
            "ORACLE_ECHO":     str("0"),
        }

    def __init__(
        self,
        user: str = "",
        password: str = "",
        host: str = "",
        port: int | str = "",
        service: str = "",
        echo: bool | None = None,
    ) -> None:
        cfg = self.get_env()

        final_user = user or cfg["ORACLE_USER"]
        final_pass = password or cfg["ORACLE_PASSWORD"]
        final_host = host or cfg["ORACLE_HOST"]
        final_port = port or cfg["ORACLE_PORT"]
        final_srv  = service or cfg["ORACLE_SERVICE"]
        final_echo = bool(int(cfg["ORACLE_ECHO"])) if echo is None else bool(echo)

        uri = (
            f"oracle+oracledb://{final_user}:{final_pass}"
            f"@{final_host}:{final_port}/?service_name={final_srv}"
        )

        print("Tworzenie engine SQLAlchemy (Oracle).")
        self.engine: Engine = create_engine(uri, echo=final_echo, future=True)
        self._ensure_schema()

    # ------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------

    def _ensure_schema(self) -> None:
        with self.engine.begin() as con:

            con.execute(text("""
            BEGIN
                EXECUTE IMMEDIATE '
                    CREATE TABLE pmps_act_devices (
                        id NUMBER PRIMARY KEY,
                        area VARCHAR2(2) NOT NULL,
                        line VARCHAR2(4) NOT NULL,
                        controller VARCHAR2(1) NOT NULL,
                        circuit VARCHAR2(1) NOT NULL,
                        station VARCHAR2(4) NOT NULL,
                        device VARCHAR2(3) NOT NULL,
                        detail VARCHAR2(4) NOT NULL,
                        serial VARCHAR2(15),
                        health NUMBER,
                        n_clusters NUMBER,
                        warning NUMBER,
                        alert NUMBER,
                        sensitivity_min NUMBER,
                        sensitivity_max NUMBER,
                        status NUMBER(1) NOT NULL,
                        updated_at TIMESTAMP NOT NULL,
                        CONSTRAINT uq_pmps_device UNIQUE (
                            area, line, controller, circuit, station, device, detail
                        )
                    )
                ';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN RAISE; END IF;
            END;
            """))

            con.execute(text("""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE SEQUENCE pmps_act_devices_seq START WITH 1 INCREMENT BY 1';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN RAISE; END IF;
            END;
            """))

            con.exec_driver_sql("""
            BEGIN
                EXECUTE IMMEDIATE '
                    CREATE OR REPLACE TRIGGER trg_pmps_act_devices
                    BEFORE INSERT ON pmps_act_devices
                    FOR EACH ROW
                    WHEN (new.id IS NULL)
                    BEGIN
                        SELECT pmps_act_devices_seq.NEXTVAL INTO :new.id FROM dual;
                    END;
                ';
            END;
            """)

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------

    @staticmethod
    def _split_device_code(device_code: str) -> dict:
        if not isinstance(device_code, str) or len(device_code) < 22:
            raise ValueError(f"Niepoprawny kod urządzenia: {device_code}")

        return {
            "area": device_code[3:5],
            "line": device_code[5:9],
            "controller": device_code[9:10],
            "circuit": device_code[10:11],
            "station": device_code[11:15],
            "device": device_code[15:18],
            "detail": device_code[18:22],
        }

    # ------------------------------------------------------------
    # CONFIG (STANDARD)
    # ------------------------------------------------------------

    def get_config(self, typ: str, config_id: int = 1) -> Optional[Dict[str, int]]:
        sql = f"""
            SELECT warning, alert, n_clusters, sensitivity_min, sensitivity_max
            FROM pmps_{typ}_config
            WHERE id = :id
            FETCH FIRST 1 ROWS ONLY
        """
        with self.engine.begin() as con:
            row = con.execute(text(sql), {"id": config_id}).fetchone()
        return None if row is None else dict(row._mapping)

    # ------------------------------------------------------------
    # CONFIG (FROM DEVICE)
    # ------------------------------------------------------------

    def get_device_config(self, device_code: str, typ: str = "act") -> Optional[Dict[str, int]]:
        parts = self._split_device_code(device_code)

        sql = f"""
            SELECT n_clusters, sensitivity_min, sensitivity_max
            FROM pmps_{typ}_devices
            WHERE area=:area
            AND line=:line
            AND controller=:controller
            AND circuit=:circuit
            AND station=:station
            AND device=:device
            AND detail=:detail
        """

        with self.engine.begin() as con:
            row = con.execute(text(sql), parts).fetchone()

            if not row:
                return None

            return {
                "n_clusters": int(row.n_clusters) if row.n_clusters is not None else None,
                "sensitivity_min": int(row.sensitivity_min) if row.sensitivity_min is not None else None,
                "sensitivity_max": int(row.sensitivity_max) if row.sensitivity_max is not None else None,
            }

    # ------------------------------------------------------------
    # STATUS (CHECK)
    # ------------------------------------------------------------

    def get_model_status(self, device_code: str, typ: str) -> Optional[Tuple[int, datetime]]:
        parts = self._split_device_code(device_code)

        check_sql = """
            SELECT COUNT(*) 
            FROM user_tab_columns 
            WHERE table_name = :table_name
            AND column_name = 'STATUS'
        """

        table_name = f"PMPS_{typ.upper()}_DEVICES"

        with self.engine.begin() as con:
            exists = con.execute(
                text(check_sql),
                {"table_name": table_name}
            ).scalar()

            if exists == 0:
                return None

            sql = f"""
            SELECT status, fixed_at
            FROM pmps_{typ}_devices
            WHERE area=:area
            AND line=:line
            AND controller=:controller
            AND circuit=:circuit
            AND station=:station
            AND device=:device
            AND detail=:detail
            """

            row = con.execute(text(sql), parts).fetchone()

            if not row:
                return None

            status, fixed_at = row

            if status is None:
                return None

            try:
                status = int(status)
            except (TypeError, ValueError):
                return None

            return (status, fixed_at) if status >= 0 else None


    def upsert_model_status(
        self,
        device_code: str,
        typ: str,
        config: bool = True,
        status: Optional[int] = None,
        updated_at: Optional[datetime] = None,
        health: Optional[int] = None,
        serial: Optional[str] = None,
        sort: Optional[str] = None,  
    ) -> None:

        parts = self._split_device_code(device_code)
        parts["updated_at"] = updated_at or datetime.now()
        parts["status"] = int(status) if status is not None else 0

        # -------------------------
        # TYPE-SPECIFIC LOGIC
        # -------------------------

        if typ.upper() == "AIR":
            if not sort:
                raise ValueError("SORT is required for AIR")

            parts["sort"] = str(sort)

            # upewniamy się że serial NIE istnieje
            parts.pop("serial", None)

        else:  # ACT
            if serial is not None:
                parts["serial"] = serial[:15]

        # -------------------------
        # CONFIG
        # -------------------------

        if config:
            cfg = self.get_config(typ)
            if not cfg:
                raise ValueError("Brak konfiguracji")
            parts.update(cfg)

        if health is not None:
            parts["health"] = int(health)

        # -------------------------
        # KEY COLUMNS
        # -------------------------

        key_cols = {"area", "line", "controller", "circuit", "station", "device", "detail"}

        if typ.upper() == "AIR":
            key_cols.add("sort")  

        # -------------------------
        # SQL BUILD
        # -------------------------

        cols = ", ".join(list(parts.keys()) + ["fixed_at"])
        src = ", ".join(f":{k} AS {k}" for k in parts)

        update_set = ",\n                ".join(
            f"{k}=s.{k}" for k in parts.keys() if k not in key_cols
        )

        if config:
            update_set += ",\n                fixed_at = SYSTIMESTAMP"

        insert_vals = ", ".join(f"s.{k}" for k in parts.keys()) + ", SYSTIMESTAMP"

        # dynamiczne ON (bo AIR ma dodatkowe pole)
        on_clause = " AND ".join([f"t.{k}=s.{k}" for k in key_cols])

        sql = f"""
            MERGE INTO pmps_{typ}_devices t
            USING (SELECT {src} FROM dual) s
            ON ({on_clause})
            WHEN MATCHED THEN UPDATE SET
                    {update_set}
            WHEN NOT MATCHED THEN INSERT ({cols})
            VALUES ({insert_vals})
        """

        print(f"[UPSERT {typ}] parts={parts}")

        with self.engine.begin() as con:
            con.execute(text(sql), parts)

    # ------------------------------------------------------------
    # DATA
    # ------------------------------------------------------------

    def insert_device_data(
        self,
        device_code: str,
        yr: int,
        vr: int,
        yv: int,
        rv: int,
        health: int,
        _timestamp: datetime,
    ) -> None:

        parts = self._split_device_code(device_code)

        sql_id = """
            SELECT id FROM pmps_act_devices
            WHERE area=:area AND line=:line AND controller=:controller
              AND circuit=:circuit AND station=:station
              AND device=:device AND detail=:detail
            FETCH FIRST 1 ROWS ONLY
        """

        with self.engine.begin() as con:
            row = con.execute(text(sql_id), parts).fetchone()
            if not row:
                raise ValueError("Urządzenie nie istnieje")

            con.execute(
                text("""
                INSERT INTO pmps_act_data (
                    id, device_id, avg_yr, avg_vr, avg_yv, avg_rv, health, updated_at
                ) VALUES (
                    PMPS_ACT_DATA_SEQ.NEXTVAL, :device_id, :yr, :vr, :yv, :rv, :health, :ts
                )
                """),
                {
                    "device_id": row.id,
                    "yr": yr,
                    "vr": vr,
                    "yv": yv,
                    "rv": rv,
                    "health": health,
                    "ts": _timestamp,
                },
            )

    def insert_flow_data(
        self,
        device_code: str,
        sort: str,
        flow: int,
        cycle: int,
        health: int,
        _timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Inserts flow/cycle data into PMPS_AIR_DATA table.
        Uses PMPS_AIR_DEVICES to resolve device_id.
        """

        parts = self._split_device_code(device_code)

        # Dodajemy sort do wyszukiwania urządzenia
        parts["sort"] = sort

        sql_device = """
            SELECT ID FROM PMPS_AIR_DEVICES
            WHERE AREA = :area
            AND LINE = :line
            AND CONTROLLER = :controller
            AND CIRCUIT = :circuit
            AND STATION = :station
            AND DEVICE = :device
            AND DETAIL = :detail
            AND SORT = :sort
            FETCH FIRST 1 ROWS ONLY
        """

        with self.engine.begin() as con:
            result = con.execute(text(sql_device), parts).fetchone()

            if not result:
                raise ValueError(
                    f"Device not found in PMPS_AIR_DEVICES: "
                    f"{device_code}, station={station}, sort={sort}"
                )

            device_id = result[0]

            sql_insert = """
                INSERT INTO PMPS_AIR_DATA (
                    DEVICE_ID,
                    SORT,
                    AVG_FLOW,
                    AVG_CYCLE,
                    HEALTH,
                    UPDATED_AT
                ) VALUES (
                    :device_id,
                    :sort,
                    :flow,
                    :cycle,
                    :health,
                    :updated_at
                )
            """

            con.execute(
                text(sql_insert),
                {
                    "device_id": int(device_id),
                    "sort": str(sort),
                    "flow": int(flow),
                    "cycle": int(cycle),
                    "health": int(health),
                    "updated_at": _timestamp or datetime.now(),
                },
            )

        print(
            f"[{device_code}] AIR data saved: "
            f"sort={sort}, flow={flow}, cycle={cycle}, health={health}"
        )
