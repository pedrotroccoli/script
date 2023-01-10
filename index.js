const mariadb = require("mariadb");

const wait = async (time) => {
  return await new Promise((resolve) => {
    setTimeout(resolve, time * 1000);
  });
};

async function asyncFunction() {
  console.log("Criar ponte");

  const pool = mariadb.createPool({
    host: "localhost",
    user: "root",
    password: "root",
    port: 3306,
    connectionLimit: 5,
    database: "test",
  });

  console.log("Ponte criada");

  await wait(0.5);

  let connection;

  console.log("Criar conexão");

  connection = await pool.getConnection();

  console.log("Conexão criada");

  await wait(0.5);

  try {
    console.log("Configurar tabela");

    await connection.query("DROP TABLE IF EXISTS pessoasFisicasComCNPJ");

    await connection.query(
      "CREATE TABLE pessoasFisicasComCNPJ (name varchar(255), cpf varchar(255), cnpj varchar(255))"
    );

    console.log("Tabela configurada");

    await wait(0.5);

    console.log("Analisar pessoas físicas");

    const queryPF = await connection.query("SELECT * FROM pessoasfisicas");

    console.log("Pessoas fisicas analisadas");

    await wait(0.5);

    const pessoasFisicas = transformInArray(queryPF);

    let actualIndex = 0;

    async function searchPrimoInBr(nome, cpf) {
      if (!nome || !cpf) return;

      console.log("Procurar pessoa juridica", nome);

      try {
        console.log("Procurando");

        const queryPJ = await connection.query(
          `SELECT * FROM pessoasjuridicas WHERE cnpj_cpf_socio='${cpf.replace(
            /-/g,
            ""
          )}'`
        );

        const cnpjs = transformInArray(queryPJ).map((item) => item.cnpj);

        if (!cnpjs.length) {
          throw new Error("Sem CNPJ");
        }

        const insertData = [nome, cpf, cnpjs.join("^")];

        await connection.query(
          "INSERT INTO pessoasFisicasComCNPJ VALUES (?, ?, ?)",
          insertData
        );

        console.log("sucesso", nome);
      } catch (error) {
        console.log(nome);
      }

      if (pessoasFisicas.length > actualIndex) {
        actualIndex += 1;

        let search = pessoasFisicas[actualIndex];

        if (!search) {
          return;
        }

        await wait(0.5);

        searchPrimoInBr(
          pessoasFisicas[actualIndex]["Nome"],
          pessoasFisicas[actualIndex]["CPF"]
        );
      }
    }

    searchPrimoInBr(
      pessoasFisicas[actualIndex]["Nome"],
      pessoasFisicas[actualIndex]["CPF"]
    );
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
