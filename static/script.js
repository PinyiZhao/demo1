const runCommand = "http://127.0.0.1:8080/run-command"
const runAdd = "http://127.0.0.1:8080/run-add"
const runDel = "http://127.0.0.1:8080/run-del"
const runUpd = "http://127.0.0.1:8080/run-upd"

const nosqlBotton = document.getElementById("NoSQL-btn")
const relationBotton = document.getElementById("Relational-btn")
const addBotton = document.getElementById("add-btn")
const deleteBotton = document.getElementById("delete-btn")
const updateBotton = document.getElementById("update-btn")



document.getElementById('run-btn').addEventListener('click', async function() {
  const code = document.getElementById('code-input').value.trim();
  const response = await axios.get(runCommand, { params: {command: code}});
  console.log(response);
  // Here you would have the logic to run the code and get the output
  // For demonstration purposes, we will just echo the input
  const output = response['data']['output'];

  document.getElementById('console-output').textContent = output;
});

document.getElementById('select-btn').addEventListener('click', async function() {
  const code = document.getElementById('code-input').value.trim();
  const response = await axios.get(runCommand, { params: {command: code}});
  const res = response["data"]["output"];
  const tableHead = document.getElementById('table-header');
  const tableBody = document.getElementById('data-table').getElementsByTagName('tbody')[0];
   if (tableHead) {
      tableHead.innerHTML = "";
  }

  if (tableBody) {
      tableBody.innerHTML = "";
  }

  const keys = Object.keys(res[0]);
  keys.forEach(key => {
            let header = document.createElement('th');
            header.innerHTML = key;
            tableHead.appendChild(header);
        });
  res.forEach(item => {
            let row = tableBody.insertRow();

            keys.forEach(key => {
                let cell = row.insertCell();
                cell.innerHTML = item[key];
            });
        });
});

async function addBottonHandle() {
  const addString = document.getElementById('code-input2').value.trim();
  const addCommand = "insert into " + addString
  console.log(addCommand)
  const response = await  axios.get(runAdd, {params: {command: addCommand}})
  console.log(response)
  document.getElementById('console-output2').textContent = response["data"];
}
async function deleteBottonHandle() {
  const delString = document.getElementById('code-input2').value.trim();
  const delCommand = "delete from " + delString
  console.log(delCommand)
  const response = await  axios.get(runDel, {params: {command: delCommand}})
  console.log(response)
  document.getElementById('console-output2').textContent = response["data"];
}
async function updateBottonHandle() {
  const updString = document.getElementById('code-input2').value.trim();
  const updCommand = "update " + updString
  console.log(updCommand)
  const response = await  axios.get(runUpd, {params: {command: updCommand}})
  console.log(response)
  document.getElementById('console-output2').textContent = response["data"];
}

async function nosql() {
    window.location.href = 'http://127.0.0.1:8080';
}
async function mysql() {
    window.location.href = 'http://127.0.0.1:8080/ysh';
}

addBotton.addEventListener("click", addBottonHandle);
deleteBotton.addEventListener("click", deleteBottonHandle);
updateBotton.addEventListener("click", updateBottonHandle);
nosqlBotton.addEventListener("click", nosql);
relationBotton.addEventListener("click", mysql);