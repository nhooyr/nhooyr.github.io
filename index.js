const { host, pathname } = window.location
document.getElementById("current_path").textContent = pathname
document.title = host + (pathname === "/" ? "" : pathname)
