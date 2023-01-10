const mariadb = require("mariadb");

const pool = mariadb.createPool({
  host: "localhost",
  user: "root",
  password: "root",
  port: 3306,
  connectionLimit: 5,
  database: "teste",
});

async function asyncFunction() {
  let connection;

  connection = await pool.getConnection();

  try {
    await connection.query("DROP TABLE IF EXISTS pessoasFisicasComCNPJ");

    await connection.query(
      "CREATE TABLE pessoasFisicasComCNPJ (name varchar(255), cpf varchar(255), cnpj varchar(255))"
    );

    const queryPF = await connection.query("SELECT * FROM pessoasfisicas");

    const pessoasFisicas = transformInArray(queryPF);

    async function searchPrimoInBr(nome, cpf) {
      const queryPJ = await connection.query(
        `SELECT * FROM pessoasjuridicas WHERE cnpj_cpf_socio='${cpf}'`
      );

      const cnpjs = transformInArray(queryPJ).map((item) => item.cnpj);

      if (!cnpjs.length) return;

      const insertData = [nome, cpf, cnpjs.join("^")];

      await conn.query(
        "INSERT INTO pessoasFisicasComCNPJ VALUES (?, ?, ?)",
        insertData
      );

      console.log("sucesso", nome);
    }

    pessoasFisicas.forEach(({ Nome, CPF }) => {
      searchPrimoInBr(Nome, CPF.replace(/-/g, ""));
    });
  } catch (error) {
    console.log(error);
  } finally {
    if (connection) connection.release(); //release to pool
  }
}

function transformInArray(data) {
  const { meta, ...itens } = data;

  return Object.values(itens);
}

asyncFunction();
