//go:build ignore

package main

import (
	"bytes"
	"image"
	"image/color"
	"image/png"

	"fyne.io/systray"
)

func createIcon() []byte {
	const size = 32
	img := image.NewRGBA(image.Rect(0, 0, size, size))

	// Тёмный фон
	bg := color.RGBA{R: 20, G: 27, B: 31, A: 255}
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			img.Set(x, y, bg)
		}
	}

	// Оранжевая "M"
	fg := color.RGBA{R: 255, G: 107, B: 53, A: 255}
	for y := 6; y < 26; y++ {
		for x := 7; x < 10; x++ {
			img.Set(x, y, fg)
		}
	}
	for y := 6; y < 26; y++ {
		for x := 22; x < 25; x++ {
			img.Set(x, y, fg)
		}
	}
	for i := 0; i < 10; i++ {
		x := 7 + i
		y := 6 + i
		for dx := 0; dx < 3; dx++ {
			img.Set(x+dx, y, fg)
		}
	}
	for i := 0; i < 10; i++ {
		x := 22 - i
		y := 6 + i
		for dx := 0; dx < 3; dx++ {
			img.Set(x-dx, y, fg)
		}
	}

	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return buf.Bytes()
}

func main() {
	systray.Run(func() {
		systray.SetIcon(createIcon())
		systray.SetTitle("Test")
		systray.SetTooltip("Тестовая иконка")

		mQuit := systray.AddMenuItem("Выход", "")
		go func() {
			<-mQuit.ClickedCh
			systray.Quit()
		}()
	}, nil)
}
