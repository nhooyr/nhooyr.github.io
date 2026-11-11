document.getElementById("current_path").innerText = window.location.pathname;
if (document.title == "") {
  document.title = window.location.host + window.location.pathname;
}
