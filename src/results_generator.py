import time
from urllib.parse import urlparse
import yaml

hosts_refs = dict()
with open('../config.yml') as f:
    config = yaml.safe_load(f)

with open('../index_{}.html'.format(time.time()), 'w') as out:
    out.write('<html>')
    out.write('<head>')
    out.write('<title>Sites</title>')
    out.write('</head>')
    out.write('<body>')
    with open(config['params']['sitesLogFile']) as inp:
        lines = inp.readlines()
        out.write('<p>Total: <strong>{}</strong></p>'.format(len(lines)))
        for line in lines:
            args = line.split(';')
            loc, parent = args[0], args[1] if len(args) > 1 else ''
            hostname = urlparse(parent).hostname

            if hostname not in hosts_refs:
                hosts_refs[hostname] = 1
            else:
                hosts_refs[hostname] += 1

            out.write('<p><a href="{}" target="_blank">{}</a></p>'.format(loc, loc))

    out.write('<br><hr>')
    out.write('<table border="1">')
    out.write('<tr>')
    out.write('<th>Host</th>')
    out.write('<th>Out links</th>')
    out.write('</tr>')
    for key, val in hosts_refs.items():
        out.write('<tr>')
        out.write('<td>{}</td><td>{}</td>'.format(key, val))
        out.write('</tr>')
    out.write('</table>')

    out.write('</body>')
    out.write('</html>')
