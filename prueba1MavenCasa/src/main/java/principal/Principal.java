package principal;

import java.math.BigInteger;

import org.hibernate.JDBCException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;

import java.util.List;
import java.util.Set;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import datos.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class Principal {
	private static SessionFactory sesion;

	public static void main(String[] args) {
		LogManager.getLogManager().reset();
		Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
		globalLogger.setLevel(java.util.logging.Level.OFF);

		sesion = Conexion.getSession(); // Creo la sessionFactory una única vez.
		// insertardepartamento();

		/*
		 * cargardeparget(BigInteger.valueOf(10)); cargardepar(BigInteger.valueOf(10));
		 * 
		 * cargardeparget(BigInteger.valueOf(88)); cargardepar(BigInteger.valueOf(88));
		 */

		// insertamodifdepart(BigInteger.valueOf(10), "Nuevo 10", " Nueva loc");
		// insertamodifdepart(BigInteger.valueOf(15), "Nuevo 15", " Nueva loc15");

		// actualizardepalempleado(BigInteger.valueOf(222), BigInteger.valueOf(30));
		// //OK

		// actualizardepalempleado(BigInteger.valueOf(12345), BigInteger.valueOf(20));
		// //Mal emple

//		actualizardepalempleado(BigInteger.valueOf(111), BigInteger.valueOf(200)); //Mal dep

		// Actualizar un emple con un departamento nuvo creado ahora
		// Departamentos dd = new Departamentos();
		// dd.setDeptNo(BigInteger.valueOf(25));
		// dd.setDnombre("Nuevo 25");
		// dd.setLoc("Nueva loc 25");
		// actualizaemple(BigInteger.valueOf(111), dd);
		// insertaempleadoalsetdedepartamento(BigInteger.valueOf(10),BigInteger.valueOf(7900));

		// borrardepar(BigInteger.valueOf(10));
		// borrardepar(BigInteger.valueOf(61));

		// consultauniqueresul();
		// listardepartamentos();

		// consultasconparametros();

		// consultasobjetos();
		// apellido, salario, dept
		// updatedelete("AAAA", 2000.0, 10);

		// modificaemplesalario(7521, 200); //

		// modificaemplesalario(1234, 200); //

		// modificaempledepart(7521, 20); //Existe
		// modificaempledepart(222, 20); //

		// modificaempledepart(222, 2110); //

		// modificaempledepart(7521,10); //No Existe

		insertardepardenuevos();
		sesion.close();
	}

	private static void insertardepardenuevos() {
		Session session = sesion.openSession();
		System.out.println("\nInsertar nuevos departamentos\n-----------------------");
		try {
			Transaction tx = session.beginTransaction();

			String hqlInsert = "insert into Departamentos (deptNo, dnombre, loc)"
					+ " select n.deptNo, n.dnombre, n.loc from Nuevos n";

			int filascreadas = session.createMutationQuery(hqlInsert).executeUpdate();

			tx.commit(); // valida la transacción

			System.out.printf("FILAS INSERTADAS: %d%n", filascreadas);
		} catch (jakarta.persistence.PersistenceException e) {
			System.out.println("\nERROR AL INSERTAR. YA EXISTEN");
			System.out.println(e.getMessage());

		}

		session.close();
	}

	private static void modificaempledepart(int empno, int dep) {
		Session session = sesion.openSession();

		try {
			Transaction tx = session.beginTransaction();
			String hql = "update Empleados set departamentos.deptNo = :dep where empNo=:empno";
			int filasModif = session.createMutationQuery(hql).setParameter("dep", dep).setParameter("empno", empno)
					.executeUpdate();
			System.out.println("Filas modificadas: " + filasModif);
			if (filasModif == 0)
				System.out.println("Empleado no existe: " + empno);

			tx.commit();

		} catch (jakarta.persistence.PersistenceException e) {
			System.out.println("ERROR DE PK. NO EXISTE EL DEP");
			System.out.println(e.getMessage());

		}
		session.close();

	}

	private static void modificaemplesalario(int empno, int subida) {
		Session session = sesion.openSession();
		Transaction tx = session.beginTransaction();

		String hql = "update Empleados set salario = salario + :subida where empNo=:empno";
		int filasModif = session.createMutationQuery(hql).setParameter("subida", subida).setParameter("empno", empno)
				.executeUpdate();
		System.out.println("Filas modificadas: " + filasModif);
		if (filasModif == 0)
			System.out.println("Empleado no existe: " + empno);

		tx.commit();
		session.close();

	}

	private static void updatedelete(String ape, double salario, int dep) {
		Session session = sesion.openSession();
		Transaction tx = session.beginTransaction();
		// Modificamos el salario de GIL

		String hqlModif = "update Empleados e set e.salario = :nuevoSal where e.apellido = :ape";
		int filasModif = session.createMutationQuery(hqlModif).setParameter("nuevoSal", salario)
				.setParameter("ape", ape).executeUpdate();

		System.out.println("FILAS MODIFICADAS: " + filasModif); // Nº entidades afectadas

		// Eliminamos los empleados del departamento 20

		String hqlDel = "delete Empleados e where e.departamentos.deptNo = ?1";
		int filasDel = session.createMutationQuery(hqlDel).setParameter(1, dep).executeUpdate();

		System.out.println("FILAS ELIMINADAS: " + filasDel); // Nº entidades afectadas

		// tx.rollback(); //Deshace la transacción
		tx.commit(); // valida la transacción

		session.close();

	}

	private static void consultasobjetos() {
		Session session = sesion.openSession();

		Query cons = session.createQuery(
				"from Empleados e, Departamentos d where  e.departamentos.deptNo=d.deptNo order by e.apellido");

		List datos = cons.list();
		for (int i = 1; i < datos.size(); i++) {
			Object[] par = (Object[]) datos.get(i);
			Empleados em = (Empleados) par[0]; // objeto empleado el primero
			Departamentos de = (Departamentos) par[1]; // objeto departamento el segundo
			System.out.println(em.getApellido() + "*" + em.getSalario() + "*" + de.getDnombre() + "*" + de.getLoc());
		}
		session.close();
	}

	private static void consultasconparametros() {
		Session session = sesion.openSession();

		// El siguiente ejemplo consulta el empleado con numero 7369
		// utiliza un parámetro nombrado
		String hql = "from Empleados where empNo = :numemple";
		// UTILIZAMOS ESTA QUERY q PARA TODAS LAS CONSULTAS

		Query q = session.createQuery(hql, Empleados.class);
		q.setParameter("numemple", (short) 7369);
		Empleados emple = (Empleados) q.uniqueResult();

		System.out.println("------------------------");
		System.out.println("Empleado número 7369");

		System.out.printf("%s, %s %n", emple.getApellido(), emple.getOficio());

		// El siguiente ejemplo consulta los empleados cuyo número de departamento es 10
		// y el oficio DIRECTOR. Utiliza parámetros nombrados
		hql = "from Empleados emp where emp.departamentos.deptNo = :ndep and emp.oficio = :ofi";
		q = session.createQuery(hql);
		System.out.println("------------------------");
		System.out.println("Directores del dep 10");
		q.setParameter("ndep", (byte) 10);
		q.setParameter("ofi", "DIRECTOR");
		List<Empleados> lista = q.list();
		emple = new Empleados();
		for (int i = 0; i < lista.size(); i++) {
			emple = lista.get(i);
			System.out.println(emple.getApellido());
		}

		// El mismo ejemplo con parámetros posicionales de estilo JDBC (el uso de estos
		// parámetros se considera obsoleto por lo que se recomienda usar los parámetros
		// nombrados) quedaría así:

		hql = "from Empleados emp where emp.departamentos.deptNo = ?1 and emp.oficio = ?2";
		q = session.createQuery(hql, Empleados.class);
		q.setParameter(1, (byte) 10);
		q.setParameter(2, "DIRECTOR");
		System.out.println("------------------------");
		System.out.println("Directores del dep 10, dos ");
		List<Empleados> lista2 = q.list();
		emple = new Empleados();
		for (int i = 0; i < lista2.size(); i++) {
			emple = lista2.get(i);
			System.out.println(emple.getApellido());
		}

		// El siguiente ejemplo Obtiene los empleados cuya fecha de alta es
		// 1991-12-03. Utiliza parámetro nombrado:
		SimpleDateFormat formatoDelTexto = new SimpleDateFormat("yyyy-MM-dd");
		String strFecha = "1991-12-03";
		formatoDelTexto.setLenient(false);
		java.util.Date fecha = null;
		try {
			fecha = (Date) formatoDelTexto.parse(strFecha);
			hql = "from Empleados where fechaAlt = :fechalta";
			q = session.createQuery(hql, Empleados.class);
			q.setParameter("fechalta", fecha);

			List<Empleados> lista4 = q.list();

			System.out.println("------------------------");
			System.out.println("Empleados con fecha alta: " + strFecha);
			for (int i = 0; i < lista4.size(); i++) {
				emple = lista4.get(i);
				System.out.println(emple.getApellido());
			}
		} catch (ParseException ex) {
			System.out.println("FECHA ERRÓNEA. NO SE PUEDE CONSULTAR");
			ex.printStackTrace();
		}

		// Lista de parámetros nombrados
		// El siguiente ejemplo asigna a un parámetro nombrado llamado :listadep una
		// colección de valores llamada numeros con los valores 10 y 20 para obtener
		// aquellos empleados cuyo número de departamento sea 10 o 20; se usa el método
		// setParameerList():
		List<BigInteger> numeros = new ArrayList<BigInteger>();
		numeros.add(BigInteger.valueOf(10));
		numeros.add(BigInteger.valueOf(20));

		hql = "from Empleados emp where emp.departamentos.deptNo in (:listadep) order by emp.departamentos.deptNo ";
		q = session.createQuery(hql, Empleados.class);
		q.setParameterList("listadep", numeros);

		List<Empleados> lista3 = q.list();
		System.out.println("------------------------");
		System.out.println("Empleados del dep 10 y 20");
		for (int i = 0; i < lista3.size(); i++) {
			emple = lista3.get(i);
			System.out.println(emple.getApellido());
		}
		session.close();

	}

	private static void consultauniqueresul() {

		Session session = sesion.openSession();
		// Visualiza los datos del departamento 10
		System.out.println("------------------------");
		Departamentos depart = (Departamentos) session
				.createQuery("from Departamentos as dep where dep.deptNo = 10", Departamentos.class).uniqueResult();

		if (depart != null)
			System.out.println(depart.getLoc() + "*" + depart.getDnombre());

		// Visualiza los datos del departamento con nombre CONTABILIDAD
		System.out.println("------------------------");
		depart = (Departamentos) session
				.createQuery("from Departamentos as dep where dep.dnombre = 'CONTABILIDAD'", Departamentos.class)
				.uniqueResult();
		if (depart != null)
			System.out.println(depart.getLoc() + "*" + depart.getDeptNo());

		System.out.println("------------------------");
		Long cont = (Long) session.createQuery("select count(*) from Empleados ", Long.class).uniqueResult();
		System.out.println("Número de empleados: " + cont);

		System.out.println("------------------------");
		Double media = (double) session
				.createQuery("select coalesce(avg(salario),0) from Empleados e where e.departamentos.deptNo=50",
						Double.class)
				.uniqueResult();
		System.out.println("Media de salario de empleados: " + media);

		System.out.println("------------------------");
		Double maxi = (double) session.createQuery("select max(salario) from Empleados ", Double.class).uniqueResult();
		System.out.println("Máximo de salario de empleados: " + maxi);

		session.close();

	}
	/*
	 * Modificamos el método listardepartamentos(), para obtener por cada departamento sus empleados, y los totales. Visualizando en salida formateada.
Por ejemplo para esta es la salida para el dep 30:


Num dep: 30 Nombre Dep:VENTAS  Localidad:BARCELONA  Número de empleados: 4
     EMPNO        APELLIDO          OFICIO       FECHAALTA         SALARIO 
---------- --------------- --------------- --------------- --------------- 
      7654          MARTÍN        VENDEDOR      1991-09-29          1600.0 
      7698           NEGRO        DIRECTOR      1991-05-01          3005.0 
      7844           TOVAR        VENDEDOR      1991-09-08          1350.0 
      7499          ARROYO        VENDEDOR      1990-02-20          1500.0 
---------- --------------- --------------- --------------- --------------- 
Total salario:                                                      7455.0 
---------- --------------- --------------- --------------- ---------------

	 */
	private static void listardepartamentos() {
		Session session = sesion.openSession();
		Departamentos depar = new Departamentos();
		System.out.println("-------------------------------");
		Query<Departamentos> q = session.createQuery("from Departamentos d order by d.deptNo", Departamentos.class);
		List<Departamentos> lista = q.list();
		int num = lista.size();
		System.out.println("Número de departamentos: " + num);
		for (int i = 0; i < num; i++) {
			// extraer el objeto
			depar = (Departamentos) lista.get(i);
			// Num dep: 30CVENTAS Localidad: BARCELONA Número de empleados: 4
			// EMPNO APELLIDO OFICIO FECHAALTA SALARIO
			// ---------- --------------- --------------- --------------- ---------------

			System.out.println("Num dep:" + depar.getDeptNo() + "   Nom dep:" + depar.getDnombre() + "   Localidad: "
					+ depar.getLoc() + "   Número de empleados: " + depar.getEmpleadoses().size());

			if (depar.getEmpleadoses().size() == 0) {
				System.out.println("     Departamento sin empleados");
				System.out.println();
			} else {
				Double ttsal = 0d;
				// Visualizar las cabeceras y los empleados
				System.out.printf("%5s %-15s %-15s %-15s %10s %n", "EMPNO", "APELLIDO", "OFICIO", "FECHAALTA",
						"SALARIO");
				System.out.printf("%5s %-15s %-15s %-15s %10s %n", "-----", "----------", "----------", "----------",
						"----------");

				Set<Empleados> listaem = depar.getEmpleadoses();
				for (Empleados empleados : listaem) {
					System.out.printf("%5s %-15s %-15s %-15s %10s %n", empleados.getEmpNo(), empleados.getApellido(),
							empleados.getOficio(), empleados.getFechaAlt(), empleados.getSalario());
					ttsal = ttsal + empleados.getSalario();

				}

				System.out.printf("%5s %-15s %-15s %-15s %10s %n", "-----", "----------", "----------", "----------",
						"----------");
				System.out.printf("%-21s %-15s %-15s %10s %n", "TOTAL SALARIO", " ", " ", ttsal);
				System.out.printf("%5s %-15s %-15s %-15s %10s %n", "-----", "----------", "----------", "----------",
						"----------");
				System.out.println();
				System.out.println();
			}

		}

		session.close();
	}

	private static void borrardepar(BigInteger nu) {
		Session session = sesion.openSession();
		System.out.println("---------------------------");

		Departamentos dep = (Departamentos) session.get(Departamentos.class, nu);
		if (dep == null) {
			System.out.println("El departamento no existe. NO SE PUEDE BORRAR.");
		} else {
			try {
				Transaction tx = session.beginTransaction();
				session.remove(dep);
				tx.commit();
				System.out.println("DEPARTAMETO BORRADO " + nu);

			} catch (jakarta.persistence.PersistenceException e) {
				e.printStackTrace();
				if (e.getMessage().contains("org.hibernate.exception.ConstraintViolationException")) {
					System.out.println("NO SE PUEDE BORRRAR. TIENE REGISTROS RELACIONADOS: " + nu);
				} else
					System.out.println("HA ocurrido un error: " + e.getMessage());
			}

		}
		session.close();
	}

	private static void insertaempleadoalsetdedepartamento(BigInteger nu, BigInteger emp) {
		Session session = sesion.openSession();

		Departamentos dep = (Departamentos) session.get(Departamentos.class, nu);
		if (dep == null) {
			System.out.println("El departamento no existe. No se puede insertar.");
		} else {
			// compruebo empleado
			Empleados emple = (Empleados) session.get(Empleados.class, emp);
			if (emple == null) {
				System.out.println("El Empleado no existe. No se puede insertar.");
			} else {
				// lo añado al set
				Transaction tx = session.beginTransaction();
				dep.getEmpleadoses().add(emple);
				System.out.println("Empleado " + emp + " añadido al departamento " + nu);
				session.merge(dep);
				tx.commit();
			}
		}
		session.close();

	}

	private static void actualizaemple(BigInteger emp, Departamentos dd) {

		Session session = sesion.openSession();
		Empleados emple = (Empleados) session.get(Empleados.class, emp);
		if (emple == null) {
			System.out.println("El Empleado no existe. No se puede actualizar.");
		} else {
			Transaction tx = session.beginTransaction();
			emple.setDepartamentos(dd);
			System.out.println("Empleado " + emp + " actualizado al departamento " + dd.getDeptNo());
			session.merge(emple);
			tx.commit();
		}

	}
	//Ejemplo. Actualizar el departamento al empleado.
	//Localizamos el empleado. Si existe, localizamos el departamento, y si existe actualizamos el objeto empleado.
	//Para actualizar el objeto podremos utilizar save() o update()

	private static void actualizardepalempleado(BigInteger emp, BigInteger nu) {

		Session session = sesion.openSession();
		Empleados emple = (Empleados) session.get(Empleados.class, emp);
		if (emple == null) {
			System.out.println("El Empleado no existe. No se puede actualizar.");
		} else {
			Departamentos dep = (Departamentos) session.get(Departamentos.class, nu);
			if (dep == null)
				System.out.println("El departamento no existe. No se puede actualizar.");
			else {
				Transaction tx = session.beginTransaction();
				emple.setDepartamentos(dep);
				System.out.println("Empleado " + emp + " actualizado al departamento " + nu);
				session.merge(emple);
				tx.commit();
			}
		}
		session.close();
	}
	//Ejemplo MODIFICAR
	////Para modificar un objeto, primero lo cargo, luego asigno los nuevos datos, y seguidamente lo almaceno.
	//Este método que recibe un número de departamento, un nombre y una localidad, y hace lo siguiente: Si el departamento existe lo actualiza, y si no existe lo crea.
	//Para llamar al método escribiremos:

	private static void insertamodifdepart(BigInteger nu, String nom, String loc) {

		Session session = sesion.openSession();
		Transaction tx = session.beginTransaction();
		System.out.println("Cargo departamento.");
		Departamentos dep = (Departamentos) session.get(Departamentos.class, nu);
		System.out.println("==============================");
		System.out.println("DATOS DEL DEPARTAMENTO " + nu);
		if (dep == null) {
			System.out.println("El departamento no existe, LO CREO");
			dep = new Departamentos();
			dep.setDeptNo(nu);
			dep.setDnombre(nom);
			dep.setLoc(loc);
			session.persist(dep);
		} else {
			System.out.println("El departamento existe, LO MODIFICO");
			dep.setDnombre(nom);
			dep.setLoc(loc);
			session.persist(dep);
		}
		tx.commit();
		session.close();
	}
	//EJEMPLO CON GET:
	private static void cargardeparget(BigInteger nu) {
		Session session = sesion.openSession();
		Departamentos dep = (Departamentos) session.get(Departamentos.class, nu);
		if (dep == null) {
			System.out.println("El departamento no existe");
		} else {
			System.out.println("Nombre Dep:" + dep.getDnombre());
			System.out.println("Localidad:" + dep.getLoc());
		}
		session.close();
	}
	//EJEMPLO CON LOAD:
	private static void cargardepar(BigInteger nu) {
		Session session = sesion.openSession();
		try {
			Departamentos dep = (Departamentos) session.getReference(Departamentos.class, nu);
			System.out.println("Nombre: " + dep.getDnombre());
			System.out.println("Localidad: " + dep.getLoc());
		} catch (ObjectNotFoundException ob) {
			System.out.println("NO EXISTE EL DEPARTAMENTO.");
		}
		session.close();
	}

	//Ejemplo: INSERTAR EN DEPARTAMENTOS.
	private static void insertardepartamento() {
		Session session = sesion.openSession(); // creo una sesión de trabajo
		Transaction tx = session.beginTransaction();

		Departamentos dep = new Departamentos();
		dep.setDeptNo(BigInteger.valueOf(63));
		dep.setDnombre("MM");
		dep.setLoc("GUADALAJARA");

		try {
			session.persist(dep);
			// tx.commit();
			System.out.println("Reg INSERTADO.");

		} catch (jakarta.persistence.PersistenceException e) {
			if (e.getMessage().contains("org.hibernate.exception.ConstraintViolationException")) {
				System.out.println("CLAVE DUPLICADA. DEPARTAMENTO YA EXISTE");
			} else if (e.getMessage().contains("org.hibernate.exception.DataException")) {
				System.out.println("ERROR EN LOS DATOS DE DEPARTAMENTO, DEMASIADOS CARACTERES");
			} else if (e.getMessage().contains("org.hibernate.exception.GenericJDBCException")) {
				System.out.println("ERROR JDBC. NO SE HA PODIDO EJECUATR LA CONSULTA");
			} else
				System.out.println("HA ocurrido un error: " + e.getMessage());

		} catch (Exception e) {
			System.out.println("ERROR NO CONTROLADO....");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		session.close(); // cierro la sesión de trabajo

	}

}
