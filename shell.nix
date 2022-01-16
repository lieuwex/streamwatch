with (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/e91ed60026951707237c22b5502f283c8da9667c.tar.gz") {});

pkgs.mkShell {
	buildInputs = [
		rust-bin.nightly."2021-07-03".complete
		openssl
		pkg-config
	];
}
