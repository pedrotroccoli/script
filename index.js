const mariadb = require("mariadb");

const variables = {
  host: "localhost",
  user: "root",
  password: "root",
  port: 3306,
  database: "teste",
};

const backgrounds = {
  yellow: "\x1b[43m",
  gray: "\x1b[100m",
  green: "\x1b[42m",
};

const colors = {
  green: `\x1b[32m`,
  yellow: `\x1b[30m`,
  cyan: `\x1b[36m`,
  white: `\x1b[37m`,
  black: "\x1b[30m",
};

const effects = {
  blink: "\x1b[5m",
  dim: "\x1b[2m",
};

async function createConnection() {
  printGroup(" Banco de dados ", [`\x1b[43m`, `\x1b[30m`]);

  await wait(1);
  console.log("Criando conexão");

  const pool = mariadb.createPool({
    host: variables.host,
    user: variables.user,
    password: variables.password,
    port: variables.port,
    connectionLimit: 5,
    database: variables.database,
  });

  await wait(1);

  console.log("Conexão criada");

  console.groupEnd();

  await wait(1);
  console.log(" ");

  return await pool.getConnection();
}

async function setupTable(connection) {
  printGroup(" Tabela ", [`\x1b[43m`, `\x1b[30m`]);

  await wait(1);
  console.log("Configurando tabela");

  await connection.query("DROP TABLE IF EXISTS pessoasFisicasComCNPJ");

  await connection.query(
    "CREATE TABLE pessoasFisicasComCNPJ (name varchar(150), cpf varchar(11), cnpjs TEXT)"
  );

  await wait(1);

  console.log("Tabela configurada");

  console.groupEnd();

  await wait(1);
  console.log(" ");
}

async function getPeople(connection) {
  printGroup(" Dados ", [`\x1b[43m`, `\x1b[30m`]);

  await wait(1);
  console.log("Analisando pessoas físicas");

  const queryPF = await connection.query(
    "SELECT * FROM filtradoPessoasFisicas"
  );

  await wait(1);

  console.log("Pessoas físicas analisadas");
  console.groupEnd();

  await wait(1);
  console.log(" ");

  return transformInArray(queryPF);
}

async function getCompaniesByCPF(connection, cpf) {
  const companies = await connection.query(
    `SELECT * FROM filtradoPessoasJuridicas WHERE cpf=(?)`,
    [cpf]
  );

  return transformInArray(companies);
}

async function insertNewPerson(connection, name, cpf, cnpjs) {
  const insertData = [name, cpf, JSON.stringify(cnpjs)];

  await connection.query(
    "INSERT INTO pessoasFisicasComCNPJ VALUES (?, ?, ?)",
    insertData
  );
}

async function start() {
  startConfiguration();

  const connection = await createConnection();

  let index = 0;
  let rejectedSum = 0;
  let rejected = 0;

  try {
    await setupTable(connection);

    const people = await getPeople(connection);

    startTimer();

    do {
      const currentPerson = people[index];

      const companies = await getCompaniesByCPF(connection, currentPerson.cpf);

      const cnpjs = companies.map((item) => item.cnpj);

      if (!cnpjs.length) {
        index += 1;
        rejected += 1;
        continue;
      }

      await insertNewPerson(
        connection,
        companies[0].name,
        currentPerson.cpf,
        cnpjs.slice(0, 30)
      );

      if (index !== 0 && index % 1000 === 0) {
        rejectedSum += rejected;

        sendGroupTimer(index, rejected);

        rejected = 0;
      }

      index += 1;
    } while (index < people.length);
  } catch (error) {
    console.log(error);
  } finally {
    endTimer(index, rejectedSum);

    closeConnection(connection);
  }
}

function startConfiguration() {
  console.log(" ");
  printLog("Configurando...", [colors.green]);
  console.log(" ");
}

function startTimer() {
  console.log(" ");
  printLog("Iniciando...", [colors.green]);
  console.log(" ");
  console.time("Tempo total");
  console.time("Tempo corrido");
}

function sendGroupTimer(index, rejected) {
  printGroup("Análise temporária", [backgrounds.gray, colors.white]);
  console.log("Analisadas:", index);
  console.log("Rejeitadas:", rejected);
  console.timeEnd("Tempo corrido");
  console.groupEnd();

  console.time("Tempo corrido");
  console.log(" ");
}

function endTimer(index, rejectedSum) {
  console.group("Total");
  console.log("Total de pessoas:", index);
  console.timeEnd("Tempo corrido");
  console.log("Total de rejeitados:", rejectedSum);
  console.timeEnd("Tempo total");
  console.groupEnd();
}

//
//
//

function printLog(message, colors) {
  console.log(colors.join(""), message, `\x1b[0m`);
}

function printGroup(message, colors) {
  console.group(colors.join(""), message, `\x1b[0m`);
  console.log(" ");
}

function closeConnection(connection) {
  if (connection) connection.release(); //release to pool

  console.log(" ");

  printLog("               ", [backgrounds.green]);
  printLog("    SUCESSO    ", [
    backgrounds.green,
    effects.blink,
    effects.dim,
    colors.black,
  ]);
  printLog("               ", [backgrounds.green]);
}

function transformInArray(data) {
  const { meta, ...itens } = data;

  return Object.values(itens);
}

async function wait(time) {
  return await new Promise((resolve) => {
    setTimeout(resolve, time * 1000);
  });
}

start();
