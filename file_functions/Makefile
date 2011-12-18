file.so: file.c
	gcc -Wall -I/usr/include/mysql -fPIC -shared -o $@ $<

install:
	sudo cp -p file.so /usr/lib64/mysql/plugin/

