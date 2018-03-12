all:
	@npm run browserify

browserify:
	@npm run browserify

webpack:
	@npm run webpack

clean:
	@npm run clean

lint:
	@npm run lint

test:
	@npm test

.PHONY: all browserify webpack clean lint test
