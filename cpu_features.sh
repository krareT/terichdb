
if [ `uname` == Darwin ]; then
	sysctl -a machdep.cpu |
		sed -E -n '/^machdep\.cpu\.features:[[:space:]]*/s/^[^:]*:[[:space:]]*//p' |
	   	tr 'A-Z' 'a-z' | sed -E 's/[[:space:]]+/'$'\\\n/g'
else
	cat /proc/cpuinfo | sed -n '/^flags\s*:\s*/s/^[^:]*:\s*//p' | uniq | tr 'A-Z' 'a-z' | sed 's/\s\+/\n/g'
fi
